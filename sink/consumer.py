"""
sink/consumer.py
────────────────
Sink consumer: reads GroupRaw, VenueRaw, and EventRaw messages from Kafka
and upserts them into Postgres. Uses ON CONFLICT DO UPDATE so it's safe to
re-run and handles duplicate messages from at-least-once delivery.

Usage:
    python -m sink.consumer
"""
import json
import os
import logging
import sys
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from shared.kafka_client import make_consumer
from shared.models import EventRaw, GroupRaw, VenueRaw
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── SQL ───────────────────────────────────────────────────────────────────────

UPSERT_GROUP = """
INSERT INTO groups (
    id, name, pro_network, city, country, lat, lon,
    member_count, meetup_url, last_scraped_at, events_scraped_at
) VALUES (
    %(group_urlname)s, %(name)s, %(pro_network)s, %(city)s, %(country)s,
    %(lat)s, %(lon)s, %(member_count)s, %(meetup_url)s, %(scraped_at)s,
    %(events_scraped_at)s
)
ON CONFLICT (id) DO UPDATE SET
    name              = EXCLUDED.name,
    city              = EXCLUDED.city,
    country           = EXCLUDED.country,
    lat               = EXCLUDED.lat,
    lon               = EXCLUDED.lon,
    member_count      = EXCLUDED.member_count,
    meetup_url        = EXCLUDED.meetup_url,
    last_scraped_at   = EXCLUDED.last_scraped_at,
    -- Only advance events_scraped_at if the events scrape succeeded.
    -- Keeps the last known-good timestamp if this run's events fetch failed.
    events_scraped_at = COALESCE(EXCLUDED.events_scraped_at, groups.events_scraped_at),
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
    -- Only update coords if we got a better geocode source this time.
    -- Priority: postcode > address > city > miss
    lat            = CASE
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.lat
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('postcode') THEN EXCLUDED.lat
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('postcode', 'address') THEN EXCLUDED.lat
                       ELSE venues.lat
                     END,
    lon            = CASE
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.lon
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('postcode') THEN EXCLUDED.lon
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('postcode', 'address') THEN EXCLUDED.lon
                       ELSE venues.lon
                     END,
    geocode_source = CASE
                       WHEN EXCLUDED.geocode_source = 'postcode' THEN EXCLUDED.geocode_source
                       WHEN EXCLUDED.geocode_source = 'address'
                            AND venues.geocode_source NOT IN ('postcode') THEN EXCLUDED.geocode_source
                       WHEN EXCLUDED.geocode_source = 'city'
                            AND venues.geocode_source NOT IN ('postcode', 'address') THEN EXCLUDED.geocode_source
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

# ── Handlers ──────────────────────────────────────────────────────────────────

def handle_group(payload: dict, conn: psycopg.Connection) -> None:
    group = GroupRaw(**payload)
    now = datetime.now(timezone.utc)
    params = group.model_dump(mode="json")
    params["events_scraped_at"] = now.isoformat() if group.events_scrape_ok else None
    with conn.cursor() as cur:
        cur.execute(UPSERT_GROUP, params)
    conn.commit()
    log.debug("Upserted group: %s (events_ok=%s)", group.group_urlname, group.events_scrape_ok)


def handle_venue(payload: dict, conn: psycopg.Connection) -> None:
    venue = VenueRaw(**payload)
    with conn.cursor() as cur:
        cur.execute(UPSERT_VENUE, venue.model_dump(mode="json"))
    conn.commit()
    log.debug("Upserted venue: %s (%s)", venue.venue_id, venue.geocode_source)


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

    # FK retry buffer — events that arrived before their venue was written.
    # Keyed by venue_id -> [event payloads].
    # Flushed when the corresponding venue message arrives.
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

                        # Flush any events that were waiting on this venue
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
                    # Venue FK violation — buffer the event until the venue arrives
                    if "events_venue_id_fkey" in err and topic == settings.topic_events_raw:
                        venue_id = payload.get("venue_id")
                        if venue_id:
                            venue_fk_retry_buffer.setdefault(venue_id, []).append(payload)
                            log.warning(
                                "FK violation: venue %s not yet written — buffering event %s",
                                venue_id, payload.get("event_id"),
                            )
                        # Don't commit — message will be redelivered if sink restarts
                    # Group FK violation on events — same pattern as before
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
    settings = Settings.from_env()
    try:
        run(settings)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()