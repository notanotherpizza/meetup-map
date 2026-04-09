"""
sink/consumer.py
────────────────
Sink consumer: reads GroupRaw, VenueRaw, and EventRaw messages from Kafka
and upserts them into Postgres. Handles all geocoding (groups and venues)
via a 3-level cache: Postgres table → geocode_cache → Nominatim.

Luma venues arrive pre-geocoded (geocode_source='luma_google') and skip
Nominatim entirely.

Uses ON CONFLICT DO UPDATE so it's safe to re-run and handles duplicate
messages from at-least-once delivery.

Usage:
    python -m sink.consumer
"""
import json
import os
import logging
import re
import sys
import time
from datetime import datetime, timezone

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.geocoding import COUNTRY_CODE_TO_NAME, NOMINATIM_URL, NOMINATIM_DELAY
from shared.kafka_client import make_consumer
from shared.models import EventRaw, GroupRaw, VenueRaw
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── Geocoding ─────────────────────────────────────────────────────────────────

POSTCODE_PATTERNS = [
    re.compile(r"^[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}$", re.I),  # UK: EC4R 3AD
    re.compile(r"^\d{5}(-\d{4})?$"),                                  # US: 10001 / 10001-1234
    re.compile(r"^[A-Z]\d[A-Z]\s*\d[A-Z]\d$", re.I),                # CA: M5V 3A8
    re.compile(r"^\d{4}\s?[A-Z]{2}$", re.I),                         # NL: 1234 AB
    re.compile(r"^\d{4,5}$"),                                          # DE/FR/AU: 10115
]


def _looks_like_postcode(s: str) -> bool:
    return any(p.match(s.strip()) for p in POSTCODE_PATTERNS)


def _nominatim_lookup(query: str) -> tuple[float | None, float | None, str | None]:
    try:
        resp = httpx.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1},
            headers={"User-Agent": "meetupmap/0.1 (github.com/notanotherpizza/meetup-map)"},
            timeout=10,
        )
        results = resp.json()
        time.sleep(NOMINATIM_DELAY)
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"]), results[0].get("display_name")
        return None, None, None
    except Exception as exc:
        log.warning("Nominatim error for '%s': %s", query, exc)
        return None, None, None


def _geocode_and_cache(
    query: str,
    source: str,
    conn: psycopg.Connection,
) -> tuple[float | None, float | None, str]:
    """Check geocode_cache, fall back to Nominatim. Returns (lat, lon, source)."""
    with conn.cursor() as cur:
        cur.execute("SELECT lat, lon FROM geocode_cache WHERE query = %s", (query,))
        row = cur.fetchone()
        if row is not None:
            return row["lat"], row["lon"], source

    lat, lon, display = _nominatim_lookup(query)
    if not lat:
        source = "miss"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO geocode_cache (query, lat, lon, display_name) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (query) DO NOTHING",
            (query, lat, lon, display),
        )
    conn.commit()
    return lat, lon, source


# ── SQL ───────────────────────────────────────────────────────────────────────

# FIX: pro_network is only updated when the incoming tag has >= priority to the
# existing one. Priority (highest first): real pro network slug (100) >
# "community" (10) > "discovered_meetup" / "discovered_luma" (5).
# Any tag not in the CASE expression is treated as a real pro network slug and
# always wins.
UPSERT_GROUP = """
INSERT INTO groups (
    id, name, pro_network, platform, city, country, lat, lon,
    member_count, source_url, last_scraped_at, events_scraped_at,
    total_past_events
) VALUES (
    %(group_urlname)s, %(name)s, %(pro_network)s, %(platform)s,
    %(city)s, %(country)s, %(lat)s, %(lon)s,
    %(member_count)s, %(source_url)s, %(scraped_at)s,
    %(events_scraped_at)s, %(total_past_events)s
)
ON CONFLICT (id) DO UPDATE SET
    name              = EXCLUDED.name,
    platform          = EXCLUDED.platform,
    city              = EXCLUDED.city,
    country           = EXCLUDED.country,
    lat               = EXCLUDED.lat,
    lon               = EXCLUDED.lon,
    member_count      = EXCLUDED.member_count,
    source_url        = EXCLUDED.source_url,
    last_scraped_at   = EXCLUDED.last_scraped_at,
    events_scraped_at = COALESCE(EXCLUDED.events_scraped_at, groups.events_scraped_at),
    total_past_events = COALESCE(EXCLUDED.total_past_events, groups.total_past_events),
    -- Only upgrade pro_network, never downgrade.
    -- Incoming priority: real slug > "community" > "discovered_*"
    -- We compute priority as an integer and take the higher value.
    pro_network       = CASE
        WHEN EXCLUDED.pro_network NOT IN (
            'community', 'discovered_meetup', 'discovered_luma'
        ) THEN EXCLUDED.pro_network  -- real pro network slug always wins
        WHEN groups.pro_network NOT IN (
            'community', 'discovered_meetup', 'discovered_luma'
        ) THEN groups.pro_network    -- existing real slug beats any lower tag
        WHEN EXCLUDED.pro_network = 'community'
             AND groups.pro_network IN ('discovered_meetup', 'discovered_luma')
        THEN EXCLUDED.pro_network    -- community beats discovered_*
        ELSE groups.pro_network      -- keep existing (equal or higher priority)
    END,
    updated_at        = now()
"""

UPSERT_VENUE = """
INSERT INTO venues (
    id, name, address, city, state, country,
    lat, lon, geocode_source, geocode_query, first_seen_at
) VALUES (
    %(venue_id)s, %(name)s, %(address)s, %(city)s, %(state)s, %(country)s,
    %(lat)s, %(lon)s, %(geocode_source)s, %(geocode_query)s, %(scraped_at)s
)
ON CONFLICT (id) DO UPDATE SET
    name           = EXCLUDED.name,
    address        = EXCLUDED.address,
    city           = EXCLUDED.city,
    state          = EXCLUDED.state,
    country        = EXCLUDED.country,
    lat            = CASE
                       WHEN EXCLUDED.geocode_source = 'luma_google' THEN EXCLUDED.lat
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.lat
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode') THEN EXCLUDED.lat
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode', 'address') THEN EXCLUDED.lat
                       ELSE venues.lat
                     END,
    lon            = CASE
                       WHEN EXCLUDED.geocode_source = 'luma_google' THEN EXCLUDED.lon
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.lon
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode') THEN EXCLUDED.lon
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode', 'address') THEN EXCLUDED.lon
                       ELSE venues.lon
                     END,
    geocode_source = CASE
                       WHEN EXCLUDED.geocode_source = 'luma_google' THEN EXCLUDED.geocode_source
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.geocode_source
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode') THEN EXCLUDED.geocode_source
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('luma_google', 'postcode', 'address') THEN EXCLUDED.geocode_source
                       ELSE venues.geocode_source
                     END,
    geocode_query  = COALESCE(venues.geocode_query, EXCLUDED.geocode_query),
    updated_at     = now()
"""

UPSERT_EVENT = """
INSERT INTO events (
    id, group_id, title, event_url, status,
    is_online, venue_id, starts_at, ends_at,
    rsvp_count, last_scraped_at
) VALUES (
    %(event_id)s, %(group_urlname)s, %(title)s,
    %(event_url)s, %(status)s, %(is_online)s, %(venue_id)s,
    %(starts_at)s, %(ends_at)s, %(rsvp_count)s, %(scraped_at)s
)
ON CONFLICT (id) DO UPDATE SET
    title           = EXCLUDED.title,
    status          = EXCLUDED.status,
    is_online       = EXCLUDED.is_online,
    venue_id        = EXCLUDED.venue_id,
    starts_at       = EXCLUDED.starts_at,
    rsvp_count      = EXCLUDED.rsvp_count,
    last_scraped_at = EXCLUDED.last_scraped_at,
    updated_at      = now()
"""

# FIX: ON CONFLICT DO NOTHING on (run_id, group_id) prevents duplicate scrape_log
# rows from at-least-once Kafka delivery. Requires the unique constraint added
# in schema.sql migration below.
INSERT_SCRAPE_LOG = """
INSERT INTO scrape_log
    (run_id, worker_id, group_id, pro_network, duration_ms)
VALUES
    (%s, %s, %s, %s, %s)
ON CONFLICT (run_id, group_id) DO NOTHING
"""


# ── run_id helper ─────────────────────────────────────────────────────────────

def _fetch_current_run_id(conn: psycopg.Connection) -> int | None:
    """
    Return the most recent scrape_runs.id. Called once per group message so
    the sink automatically picks up new runs started after the sink launched,
    without needing a restart.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM scrape_runs ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row["id"] if row else None


# ── Handlers ──────────────────────────────────────────────────────────────────

def handle_group(payload: dict, conn: psycopg.Connection) -> None:
    group = GroupRaw(**payload)

    lat, lon = group.lat, group.lon

    # For Luma groups the seed already carries lat/lon from the calendar page.
    # For Meetup groups, geocode from city + country if not already in the table.
    if lat is None:
        # L1: group already has coords in the table from a previous run
        with conn.cursor() as cur:
            cur.execute("SELECT lat, lon FROM groups WHERE id = %s", (group.group_urlname,))
            row = cur.fetchone()
            if row and row["lat"] is not None:
                lat, lon = row["lat"], row["lon"]

    # L2/L3: geocode_cache then Nominatim
    if lat is None and group.city:
        country_name = COUNTRY_CODE_TO_NAME.get(
            (group.country or "").lower(), (group.country or "").upper()
        )
        query = f"{group.city}, {country_name}" if country_name else group.city
        lat, lon, _ = _geocode_and_cache(query, "city", conn)

    now = datetime.now(timezone.utc)
    params = group.model_dump(mode="json")
    params["lat"] = lat
    params["lon"] = lon
    params["events_scraped_at"] = now.isoformat() if group.events_scrape_ok else None

    with conn.cursor() as cur:
        cur.execute(UPSERT_GROUP, params)

    # FIX: fetch run_id fresh from DB on each group message so the sink
    # automatically tracks whichever run is currently active, even if it
    # was started after the sink process launched.
    run_id = _fetch_current_run_id(conn)

    with conn.cursor() as cur:
        cur.execute(INSERT_SCRAPE_LOG, (
            run_id,
            group.worker_id,
            group.group_urlname,
            group.pro_network,
            group.scrape_duration_ms,
        ))

    conn.commit()
    log.debug("Upserted group: %s (platform=%s, events_ok=%s)",
              group.group_urlname, group.platform, group.events_scrape_ok)


def handle_venue(payload: dict, conn: psycopg.Connection) -> None:
    venue = VenueRaw(**payload)

    lat = venue.lat
    lon = venue.lon
    geocode_source = venue.geocode_source or "miss"
    geocode_query = None

    # Luma venues arrive pre-geocoded — skip Nominatim entirely
    if geocode_source == "luma_google":
        log.debug("Venue %s: using pre-geocoded Luma coords", venue.venue_id)
    elif lat is None:
        # L1: already geocoded in a previous run
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lat, lon, geocode_source, geocode_query FROM venues WHERE id = %s",
                (venue.venue_id,)
            )
            row = cur.fetchone()
            if row and row["lat"] is not None:
                lat, lon, geocode_source, geocode_query = (
                    row["lat"], row["lon"], row["geocode_source"], row["geocode_query"]
                )

        # L2/L3: geocode_cache then Nominatim
        if lat is None:
            country_name = COUNTRY_CODE_TO_NAME.get(
                (venue.country or "").lower(), (venue.country or "").upper()
            )
            if venue.name and _looks_like_postcode(venue.name):
                q = f"{venue.name.strip()}, {country_name}" if country_name else venue.name.strip()
                lat, lon, geocode_source = _geocode_and_cache(q, "postcode", conn)
                geocode_query = q
            elif venue.address and len(venue.address.strip()) > 3:
                parts = [p for p in [venue.address.strip(), venue.city, country_name] if p]
                q = ", ".join(parts)
                lat, lon, geocode_source = _geocode_and_cache(q, "address", conn)
                geocode_query = q
            elif venue.city:
                q = f"{venue.city}, {country_name}" if country_name else venue.city
                lat, lon, geocode_source = _geocode_and_cache(q, "city", conn)
                geocode_query = q

    params = venue.model_dump(mode="json")
    params["lat"] = lat
    params["lon"] = lon
    params["geocode_source"] = geocode_source
    params["geocode_query"] = geocode_query

    with conn.cursor() as cur:
        cur.execute(UPSERT_VENUE, params)
    conn.commit()
    log.debug("Upserted venue: %s (%s)", venue.venue_id, geocode_source)


def handle_event(payload: dict, conn: psycopg.Connection) -> None:
    event = EventRaw(**payload)
    with conn.cursor() as cur:
        cur.execute(UPSERT_EVENT, event.model_dump(mode="json"))
    conn.commit()
    log.debug("Upserted event: %s / %s", event.group_urlname, event.event_id)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(settings: Settings) -> None:
    consumer = make_consumer(
        settings,
        group_id="meetupmap-sink-homelab",
        topics=[
            settings.topic_groups_raw,
            settings.topic_venues_raw,
            settings.topic_events_raw,
        ],
    )

    log.info(
        "Sink started. Listening on '%s', '%s', and '%s'...",
        settings.topic_groups_raw,
        settings.topic_venues_raw,
        settings.topic_events_raw,
    )

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 6  # 6 x 5s = 30s silence

    groups_written = 0
    venues_written = 0
    events_written = 0

    venue_fk_retry_buffer: dict[str, list[dict]] = {}

    with psycopg.connect(settings.postgres_uri, row_factory=dict_row) as conn:
        try:
            while True:
                msg = consumer.poll(timeout=5.0)

                if msg is None:
                    if drain_mode:
                        empty_polls += 1
                        log.info(
                            "No messages (%d/%d)... (groups: %d, venues: %d, events: %d)",
                            empty_polls, empty_polls_needed,
                            groups_written, venues_written, events_written,
                        )
                        if empty_polls >= empty_polls_needed:
                            log.info("Topics drained — exiting.")
                            break
                    elif groups_written or venues_written or events_written:
                        log.info(
                            "Waiting... (groups: %d, venues: %d, events: %d written so far)",
                            groups_written, venues_written, events_written,
                        )
                    continue

                if msg.error():
                    log.error("Kafka error: %s", msg.error())
                    continue

                topic = msg.topic()
                try:
                    payload = json.loads(msg.value())

                    if topic == settings.topic_groups_raw:
                        handle_group(payload, conn)
                        groups_written += 1

                    elif topic == settings.topic_venues_raw:
                        handle_venue(payload, conn)
                        venues_written += 1

                        venue_id = payload.get("venue_id")
                        if venue_id and venue_id in venue_fk_retry_buffer:
                            waiting = venue_fk_retry_buffer.pop(venue_id)
                            log.info("Flushing %d buffered events for venue %s",
                                     len(waiting), venue_id)
                            for buffered in waiting:
                                try:
                                    handle_event(buffered, conn)
                                    events_written += 1
                                except Exception as exc:
                                    log.error("Failed to flush buffered event: %s", exc)

                    elif topic == settings.topic_events_raw:
                        handle_event(payload, conn)
                        events_written += 1

                    consumer.commit(msg)
                    empty_polls = 0

                except psycopg.errors.ForeignKeyViolation as exc:
                    conn.rollback()
                    err = str(exc)
                    if "events_venue_id_fkey" in err and topic == settings.topic_events_raw:
                        venue_id = payload.get("venue_id")
                        if venue_id:
                            venue_fk_retry_buffer.setdefault(venue_id, []).append(payload)
                            log.warning(
                                "FK violation: venue %s not yet written — buffering event %s",
                                venue_id, payload.get("event_id"),
                            )
                    elif topic == settings.topic_events_raw:
                        log.warning(
                            "FK violation for event %s — group not yet written, will retry",
                            msg.key(),
                        )
                    else:
                        log.error("Unexpected FK violation on %s: %s", topic, exc)

                except Exception as exc:
                    conn.rollback()
                    log.error("Failed to sink %s: %s", msg.key(), exc, exc_info=True)

        except KeyboardInterrupt:
            log.info(
                "Shutting down. Final counts — groups: %d, venues: %d, events: %d",
                groups_written, venues_written, events_written,
            )
        finally:
            consumer.close()


def main() -> None:
    settings = Settings()
    try:
        run(settings)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()