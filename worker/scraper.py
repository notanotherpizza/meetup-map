"""
worker/scraper.py
─────────────────
Stateless worker: consumes GroupSeed messages, calls three Meetup gql2
endpoints per group (groupHome, getPastGroupEvents, getUpcomingGroupEvents),
geocodes via Postgres cache, and publishes GroupRaw + VenueRaw + EventRaw messages.

Multiple workers run in parallel — all state lives in Postgres/Kafka.

Usage:
    python -m worker.scraper
"""
import asyncio
import os
import json
import logging
import re
import sys
from datetime import datetime, timezone

import httpx
import psycopg

from shared.geocoding import COUNTRY_CODE_TO_NAME, NOMINATIM_URL, NOMINATIM_DELAY
from shared.kafka_client import make_consumer, make_producer, publish
from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

MEETUP_GQL_URL = "https://www.meetup.com/gql2"

GROUP_HOME_HASH      = "012d7194e1b3746c687a04e05cdf39a25e33a7f8228bb3c563ee55432c718bee"
PAST_EVENTS_HASH     = "321388b1e4a11b17a57efe3ae7a90abfecbc703a4f4e99519772294924c21351"
UPCOMING_EVENTS_HASH = "066e3709c68718d5ce9dd909e979ac70f99835fb3722cef77756ded808d5ca08"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-worker/0.1)",
}

# Regex patterns for common postcode formats
POSTCODE_PATTERNS = [
    re.compile(r"^[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}$", re.I),  # UK: EC4R 3AD
    re.compile(r"^\d{5}(-\d{4})?$"),                                  # US: 10001 / 10001-1234
    re.compile(r"^[A-Z]\d[A-Z]\s*\d[A-Z]\d$", re.I),                # CA: M5V 3A8
    re.compile(r"^\d{4}\s?[A-Z]{2}$", re.I),                         # NL: 1234 AB
    re.compile(r"^\d{4,5}$"),                                          # DE/FR/AU: 10115
]


# ── Meetup API ────────────────────────────────────────────────────────────────

async def gql(client, operation, variables, hash_):
    resp = await client.post(
        MEETUP_GQL_URL,
        json={
            "operationName": operation,
            "variables": variables,
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": hash_}},
        },
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise ValueError(f"GQL errors in {operation}: {data['errors']}")
    return data


async def fetch_group_home(urlname: str, client: httpx.AsyncClient) -> dict:
    try:
        data = await gql(client, "groupHome",
                         {"urlname": urlname, "includePrivateInfo": False},
                         GROUP_HOME_HASH)
        return data.get("data", {}).get("groupByUrlname", {}) or {}
    except Exception as exc:
        log.warning("  groupHome failed for %s: %s", urlname, exc)
        return {}


async def fetch_events(
    urlname: str,
    client: httpx.AsyncClient,
    max_events: int,
) -> tuple[list[dict], list[dict], int | None]:
    """Returns (past_events, upcoming_events, total_past_count).
    total_past_count comes from GQL totalCount on the first page —
    accurate even when max_events caps how many we actually fetch.
    """
    now = datetime.now(timezone.utc).isoformat()
    past: list[dict] = []
    upcoming: list[dict] = []
    total_past_count: int | None = None

    cursor = None
    while max_events == 0 or len(past) < max_events:
        variables = {"urlname": urlname, "beforeDateTime": now}
        if cursor:
            variables["after"] = cursor
        data = await gql(client, "getPastGroupEvents", variables, PAST_EVENTS_HASH)
        events_data = data.get("data", {}).get("groupByUrlname", {}).get("events", {})

        # Capture totalCount from the first page only
        if total_past_count is None:
            total_past_count = events_data.get("totalCount")

        edges = events_data.get("edges", [])
        past.extend(edge["node"] for edge in edges if "node" in edge)
        page_info = events_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    data = await gql(client, "getUpcomingGroupEvents",
                     {"urlname": urlname, "afterDateTime": now}, UPCOMING_EVENTS_HASH)
    edges = (data.get("data", {})
                 .get("groupByUrlname", {})
                 .get("events", {})
                 .get("edges", []))
    upcoming.extend(edge["node"] for edge in edges if "node" in edge)

    return (past if max_events == 0 else past[:max_events]), upcoming, total_past_count


# ── Geocoding via Postgres ────────────────────────────────────────────────────

def _geocode_query(city: str | None, country: str | None) -> str | None:
    if not city:
        return None
    if country:
        country_name = COUNTRY_CODE_TO_NAME.get(country.lower(), country.upper())
        return f"{city}, {country_name}"
    return city


def _is_high_precision(lat, lon) -> bool:
    if lat is None or lon is None:
        return False
    lat_dp = len(str(abs(lat)).split(".")[-1]) if "." in str(lat) else 0
    lon_dp = len(str(abs(lon)).split(".")[-1]) if "." in str(lon) else 0
    return lat_dp > 2 or lon_dp > 2


def _looks_like_postcode(s: str) -> bool:
    """Returns True if the string matches a known postcode pattern."""
    return any(p.match(s.strip()) for p in POSTCODE_PATTERNS)


def _nominatim_lookup(query: str) -> tuple[float | None, float | None, str | None]:
    """Single Nominatim lookup — returns (lat, lon, display_name)."""
    import time
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
        log.warning("  Nominatim error for '%s': %s", query, exc)
        return None, None, None


def resolve_coords(seed: GroupSeed, pg: psycopg.Connection) -> tuple:
    """
    Three-level coordinate resolution for groups — all state in Postgres.
    1. groups table: group already has geocoded coords from a previous run
    2. geocode_cache table: city already geocoded for another group
    3. Nominatim API: genuine miss — write result back to geocode_cache
    """
    with pg.cursor() as cur:
        cur.execute("SELECT lat, lon FROM groups WHERE id = %s", (seed.group_urlname,))
        row = cur.fetchone()
        if row and _is_high_precision(row[0], row[1]):
            log.debug("  L1 coords (groups): %s", seed.group_urlname)
            return row[0], row[1]

    query = _geocode_query(seed.city, seed.country)
    if query:
        with pg.cursor() as cur:
            cur.execute("SELECT lat, lon FROM geocode_cache WHERE query = %s", (query,))
            row = cur.fetchone()
            if row is not None:
                log.debug("  L2 coords (geocode_cache): %s", query)
                return row[0], row[1]

    if not query:
        return seed.lat, seed.lon

    lat, lon, display = _nominatim_lookup(query)
    if lat:
        log.info("  L3 Nominatim (group): %s -> (%.4f, %.4f)", query, lat, lon)
    else:
        log.warning("  L3 Nominatim miss (group): %s", query)

    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO geocode_cache (query, lat, lon, display_name) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (query) DO NOTHING",
            (query, lat, lon, display),
        )
    pg.commit()
    return lat or seed.lat, lon or seed.lon


def resolve_venue_coords(
    venue_id: str,
    name: str | None,
    address: str | None,
    city: str | None,
    state: str | None,
    country: str | None,
    pg: psycopg.Connection,
) -> tuple[float | None, float | None, str, str]:
    """
    Geocode a venue — returns (lat, lon, geocode_source, geocode_query).

    Priority:
    1. Already in venues table — return cached coords immediately
    2. Name looks like a postcode — geocode "POSTCODE, country"
    3. Address is set — geocode "address, city, country"
    4. Fall back to city + country (same as group geocoding)

    All results written to geocode_cache keyed by the query string,
    so the same postcode/address hit by another group gets a cache hit.
    """
    # L1: already geocoded this venue
    with pg.cursor() as cur:
        cur.execute("SELECT lat, lon, geocode_source, geocode_query FROM venues WHERE id = %s",
                    (venue_id,))
        row = cur.fetchone()
        if row and row[0] is not None:
            log.debug("  venue L1 cache hit: %s", venue_id)
            return row[0], row[1], row[2], row[3]

    country_name = COUNTRY_CODE_TO_NAME.get((country or "").lower(), (country or "").upper())

    # L2: name looks like a postcode
    if name and _looks_like_postcode(name):
        query = f"{name.strip()}, {country_name}" if country_name else name.strip()
        source = "postcode"
    # L3: address is set and meaningful
    elif address and len(address.strip()) > 3:
        parts = [p for p in [address.strip(), city, country_name] if p]
        query = ", ".join(parts)
        source = "address"
    # L4: fall back to city + country
    elif city:
        query = f"{city}, {country_name}" if country_name else city
        source = "city"
    else:
        return None, None, "miss", None

    # Check geocode_cache before hitting Nominatim
    with pg.cursor() as cur:
        cur.execute("SELECT lat, lon FROM geocode_cache WHERE query = %s", (query,))
        row = cur.fetchone()
        if row is not None:
            log.debug("  venue L2 geocode_cache: %s", query)
            return row[0], row[1], source, query

    # Hit Nominatim
    lat, lon, display = _nominatim_lookup(query)
    if lat:
        log.info("  venue Nominatim: %s -> (%.4f, %.4f) [%s]", query, lat, lon, source)
    else:
        log.warning("  venue Nominatim miss: %s", query)
        source = "miss"

    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO geocode_cache (query, lat, lon, display_name) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (query) DO NOTHING",
            (query, lat, lon, display),
        )
    pg.commit()
    return lat, lon, source, query


# ── Message builders ──────────────────────────────────────────────────────────

def build_group_raw(
    seed: GroupSeed,
    group_home: dict,
    lat: float | None,
    lon: float | None,
    events_scrape_ok: bool,
    total_past_events: int | None = None,
) -> GroupRaw:
    member_count = (
        (group_home.get("stats") or {})
        .get("memberCounts", {})
        .get("all")
    )
    return GroupRaw(
        group_urlname=seed.group_urlname,
        name=seed.name or seed.group_urlname,
        pro_network=seed.pro_network,
        city=seed.city,
        country=seed.country,
        lat=lat,
        lon=lon,
        member_count=member_count,
        meetup_url=seed.group_url,
        scraped_at=datetime.now(timezone.utc),
        scrape_method="gql2",
        events_scrape_ok=events_scrape_ok,
        total_past_events=total_past_events,
    )


def build_venue_raw(
    venue: dict,
    pg: psycopg.Connection,
    now: datetime,
) -> VenueRaw | None:
    """
    Build a VenueRaw from a Meetup venue object, geocoding if needed.
    Returns None if the venue has no ID.
    """
    venue_id = str(venue.get("id", "")).strip()
    if not venue_id:
        return None

    name    = venue.get("name") or None
    address = venue.get("address") or None
    city    = venue.get("city") or None
    state   = venue.get("state") or None
    country = venue.get("country") or None

    lat, lon, geocode_source, geocode_query = resolve_venue_coords(
        venue_id, name, address, city, state, country, pg
    )

    return VenueRaw(
        venue_id=venue_id,
        name=name,
        address=address,
        city=city,
        state=state,
        country=country,
        lat=lat,
        lon=lon,
        geocode_source=geocode_source,
        geocode_query=geocode_query,
        scraped_at=now,
    )


def build_event_raw(
    seed: GroupSeed,
    event: dict,
    status: str,
    now: datetime,
) -> EventRaw | None:
    event_id = str(event.get("id", ""))
    if not event_id:
        return None

    starts_at = None
    if "dateTime" in event:
        try:
            starts_at = datetime.fromisoformat(event["dateTime"])
        except Exception:
            pass

    venue = event.get("venue") or {}
    venue_id = str(venue.get("id", "")).strip() or None

    is_online = (
        event.get("isOnline", False)
        or event.get("venueType") == "ONLINE"
        or "online" in (venue.get("name") or "").lower()
    )
    # Online events have no meaningful venue
    if is_online:
        venue_id = None

    going = event.get("going") or {}
    rsvp_count = going.get("totalCount") if isinstance(going, dict) else None

    return EventRaw(
        event_id=event_id,
        group_urlname=seed.group_urlname,
        title=event.get("title", ""),
        event_url=event.get("eventUrl", ""),
        status=status,
        is_online=is_online,
        venue_id=venue_id,
        starts_at=starts_at,
        rsvp_count=rsvp_count,
        scraped_at=now,
        scrape_method="gql2",
    )


# ── Core processing ───────────────────────────────────────────────────────────

async def process_seed(
    seed: GroupSeed,
    producer,
    settings: Settings,
    client: httpx.AsyncClient,
    pg: psycopg.Connection,
) -> None:
    log.info("Processing: %s", seed.group_urlname)
    t_start = asyncio.get_event_loop().time()

    events_scrape_ok = False
    past: list[dict] = []
    upcoming: list[dict] = []
    group_home: dict = {}

    try:
        group_home, (past, upcoming, total_past_count) = await asyncio.gather(
            fetch_group_home(seed.group_urlname, client),
            fetch_events(seed.group_urlname, client, settings.max_events_per_group),
        )
        events_scrape_ok = True
    except Exception as exc:
        log.warning("  Events fetch failed for %s: %s — will still write group record",
                    seed.group_urlname, exc)
        total_past_count = None
        try:
            group_home = await fetch_group_home(seed.group_urlname, client)
        except Exception:
            group_home = {}

    log.info("  -> %d/%s past, %d upcoming | members: %s | events_ok: %s",
             len(past),
             total_past_count if total_past_count is not None else "?",
             len(upcoming),
             (group_home.get("stats") or {}).get("memberCounts", {}).get("all", "?"),
             events_scrape_ok)

    lat, lon = resolve_coords(seed, pg)
    now = datetime.now(timezone.utc)

    # Publish group
    publish(
        producer,
        topic=settings.topic_groups_raw,
        value=build_group_raw(seed, group_home, lat, lon, events_scrape_ok, total_past_count).model_dump(mode="json"),
        key=seed.group_urlname,
    )

    # Identify which venues need full geocoding vs raw-only:
    # - Most recent past event venue → geocode (this is what the map plots)
    # - Upcoming event venues → geocode (precise coords for future events)
    # - All other past event venues → raw fields only, skip Nominatim
    #
    # Sort past events by dateTime descending to find the most recent
    past_sorted = sorted(
        past,
        key=lambda e: e.get("dateTime") or "",
        reverse=True,
    )
    most_recent_past_venue_id = None
    for e in past_sorted:
        v = e.get("venue") or {}
        vid = str(v.get("id", "")).strip()
        is_online = (
            e.get("isOnline", False)
            or e.get("venueType") == "ONLINE"
            or "online" in (v.get("name") or "").lower()
        )
        if vid and not is_online:
            most_recent_past_venue_id = vid
            break

    # Only geocode the next upcoming event's venue, not all upcoming.
    # This caps geocoding at 2 venues max per group (most recent past + next upcoming).
    upcoming_venue_ids = set()
    _upcoming_with_venues = [
        e for e in upcoming
        if str((e.get("venue") or {}).get("id", "")).strip()
        and not (
            e.get("isOnline", False)
            or e.get("venueType") == "ONLINE"
            or "online" in ((e.get("venue") or {}).get("name") or "").lower()
        )
    ]
    if _upcoming_with_venues:
        _upcoming_with_venues.sort(key=lambda e: e.get("dateTime") or "")
        _next_venue_id = str(_upcoming_with_venues[0].get("venue", {}).get("id", "")).strip()
        if _next_venue_id:
            upcoming_venue_ids.add(_next_venue_id)

    geocode_venue_ids = upcoming_venue_ids.copy()
    if most_recent_past_venue_id:
        geocode_venue_ids.add(most_recent_past_venue_id)

    # Publish venues + events
    # Venues are published before their events so the sink can write them first,
    # avoiding FK violations on events.venue_id.
    published_venues: set[str] = set()  # dedup within this group's batch
    published_events = 0

    for event in past + upcoming:
        status = "past" if event in past else "upcoming"
        venue = event.get("venue") or {}
        venue_id = str(venue.get("id", "")).strip()
        is_online = (
            event.get("isOnline", False)
            or event.get("venueType") == "ONLINE"
            or "online" in (venue.get("name") or "").lower()
        )

        # Publish VenueRaw once per unique venue_id per batch
        if venue_id and not is_online and venue_id not in published_venues:
            if venue_id in geocode_venue_ids:
                # Full geocoding — most recent past venue or upcoming venue
                vr = build_venue_raw(venue, pg, now)
            else:
                # Raw fields only — historical past venue, skip Nominatim
                vr = VenueRaw(
                    venue_id=venue_id,
                    name=venue.get("name") or None,
                    address=venue.get("address") or None,
                    city=venue.get("city") or None,
                    state=venue.get("state") or None,
                    country=venue.get("country") or None,
                    lat=None,
                    lon=None,
                    geocode_source=None,
                    geocode_query=None,
                    scraped_at=now,
                )
            if vr:
                publish(
                    producer,
                    topic=settings.topic_venues_raw,
                    value=vr.model_dump(mode="json"),
                    key=vr.venue_id,
                )
                published_venues.add(venue_id)

        er = build_event_raw(seed, event, status, now)
        if er:
            publish(
                producer,
                topic=settings.topic_events_raw,
                value=er.model_dump(mode="json"),
                key=f"{seed.group_urlname}:{er.event_id}",
            )
            published_events += 1

    log.info("  -> Published 1 group + %d venues + %d events for %s",
             len(published_venues), published_events, seed.group_urlname)

    # Telemetry
    run_id = os.environ.get("RUN_ID")
    worker_id = os.environ.get("WORKER_ID", __import__("socket").gethostname())
    duration_ms = int((asyncio.get_event_loop().time() - t_start) * 1000)
    try:
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_log
                    (run_id, worker_id, group_id, pro_network, events_scraped, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(run_id) if run_id else None,
                worker_id,
                seed.group_urlname,
                seed.pro_network,
                published_events,
                duration_ms,
            ))
        pg.commit()
    except Exception as e:
        log.debug("Could not write scrape_log: %s", e)
        pg.rollback()


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def run(settings: Settings) -> None:
    consumer = make_consumer(settings, group_id="meetupmap-workers",
                             topics=[settings.topic_groups_to_scrape])
    producer = make_producer(settings)
    log.info("Worker started. Listening on '%s'...", settings.topic_groups_to_scrape)

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 3

    with psycopg.connect(settings.postgres_uri) as pg:
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                while True:
                    msg = consumer.poll(timeout=5.0)
                    if msg is None:
                        if drain_mode:
                            empty_polls += 1
                            log.info("No messages (%d/%d)...", empty_polls, empty_polls_needed)
                            if empty_polls >= empty_polls_needed:
                                log.info("Topic drained — exiting.")
                                break
                        continue
                    empty_polls = 0
                    if msg.error():
                        log.error("Kafka error: %s", msg.error())
                        continue
                    try:
                        seed = GroupSeed(**json.loads(msg.value()))
                        await process_seed(seed, producer, settings, client, pg)
                        producer.flush(timeout=10)
                        consumer.commit(msg)
                        await asyncio.sleep(settings.request_delay_seconds)
                    except Exception as exc:
                        log.error("Failed to process %s: %s",
                                  msg.key(), exc, exc_info=True)
            except KeyboardInterrupt:
                log.info("Shutting down...")
            finally:
                consumer.close()


def main() -> None:
    settings = Settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()