"""
worker/scraper.py
─────────────────
Stateless worker: consumes GroupSeed messages, calls three Meetup gql2
endpoints per group (groupHome, getPastGroupEvents, getUpcomingGroupEvents),
geocodes via Postgres cache, and publishes GroupRaw + EventRaw messages.

Multiple workers run in parallel — all state lives in Postgres/Kafka.

Usage:
    python -m worker.scraper
"""
import asyncio
import os
import json
import logging
import sys
from datetime import datetime, timezone

import httpx
import psycopg

from shared.geocoding import COUNTRY_CODE_TO_NAME, NOMINATIM_URL, NOMINATIM_DELAY
from shared.kafka_client import make_consumer, make_producer, publish
from shared.models import EventRaw, GroupRaw, GroupSeed
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
    """
    Per-group metadata: member count, city, country, lat, lon, event counts.
    Uses the groupHome persisted query — same query Meetup fires on group pages.
    Returns {} on failure so callers fall back to seed data.
    """
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
) -> tuple[list[dict], list[dict]]:
    now = datetime.now(timezone.utc).isoformat()
    past: list[dict] = []
    upcoming: list[dict] = []

    cursor = None
    while max_events == 0 or len(past) < max_events:
        variables = {"urlname": urlname, "beforeDateTime": now}
        if cursor:
            variables["after"] = cursor
        data = await gql(client, "getPastGroupEvents", variables, PAST_EVENTS_HASH)
        events_data = data.get("data", {}).get("groupByUrlname", {}).get("events", {})
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

    return (past if max_events == 0 else past[:max_events]), upcoming


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


def resolve_coords(seed: GroupSeed, pg: psycopg.Connection) -> tuple:
    """
    Three-level coordinate resolution — all state in Postgres:
    1. groups table: group already has geocoded coords from a previous run
    2. geocode_cache table: city already geocoded for another group
    3. Nominatim API: genuine miss — write result back to geocode_cache
    """
    # Level 1: existing high-precision coords for this group
    with pg.cursor() as cur:
        cur.execute("SELECT lat, lon FROM groups WHERE id = %s", (seed.group_urlname,))
        row = cur.fetchone()
        if row and _is_high_precision(row[0], row[1]):
            log.debug("  L1 coords (groups): %s", seed.group_urlname)
            return row[0], row[1]

    # Level 2: city already in geocode_cache
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

    # Level 3: Nominatim
    import time
    try:
        resp = httpx.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "featuretype": "city"},
            headers={"User-Agent": "meetupmap/0.1 (github.com/notanotherpizza/meetup-map)"},
            timeout=10,
        )
        results = resp.json()
        time.sleep(NOMINATIM_DELAY)

        if results:
            lat = float(results[0]["lat"])
            lon = float(results[0]["lon"])
            display = results[0].get("display_name", "")
            log.info("  L3 Nominatim: %s → (%.4f, %.4f)", query, lat, lon)
        else:
            lat = lon = None
            display = None
            log.warning("  L3 Nominatim miss: %s", query)

        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO geocode_cache (query, lat, lon, display_name) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (query) DO NOTHING",
                (query, lat, lon, display),
            )
        pg.commit()
        return lat or seed.lat, lon or seed.lon

    except Exception as exc:
        log.warning("  Nominatim error for %s: %s", query, exc)
        return seed.lat, seed.lon


# ── Message builders ──────────────────────────────────────────────────────────

def build_group_raw(
    seed: GroupSeed,
    group_home: dict,
    lat: float | None,
    lon: float | None,
    events_scrape_ok: bool,
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
    )


def build_event_raw(seed: GroupSeed, event: dict, status: str) -> EventRaw | None:
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
    is_online = (
        event.get("isOnline", False)
        or event.get("venueType") == "ONLINE"
        or "online" in (venue.get("name") or "").lower()
    )
    going = event.get("going") or {}
    rsvp_count = going.get("totalCount") if isinstance(going, dict) else None

    return EventRaw(
        event_id=event_id,
        group_urlname=seed.group_urlname,
        title=event.get("title", ""),
        event_url=event.get("eventUrl", ""),
        status=status,
        is_online=is_online,
        venue_name=venue.get("name"),
        venue_lat=venue.get("lat"),
        venue_lon=venue.get("lon"),
        starts_at=starts_at,
        rsvp_count=rsvp_count,
        scraped_at=datetime.now(timezone.utc),
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

    # Fire groupHome concurrently with events queries.
    # Events are caught independently — a failure sets events_scrape_ok=False
    # but still allows the group record to be written with last_scraped_at set.
    events_scrape_ok = False
    past: list[dict] = []
    upcoming: list[dict] = []

    try:
        group_home, (past, upcoming) = await asyncio.gather(
            fetch_group_home(seed.group_urlname, client),
            fetch_events(seed.group_urlname, client, settings.max_events_per_group),
        )
        events_scrape_ok = True
    except Exception as exc:
        # Events fetch failed — try group_home alone so we at least write the group
        log.warning("  Events fetch failed for %s: %s — will still write group record",
                    seed.group_urlname, exc)
        try:
            group_home = await fetch_group_home(seed.group_urlname, client)
        except Exception:
            group_home = {}

    log.info("  → %d past, %d upcoming events | members: %s | events_ok: %s",
             len(past), len(upcoming),
             (group_home.get("stats") or {}).get("memberCounts", {}).get("all", "?"),
             events_scrape_ok)

    # Geocode — Postgres-backed, no local state
    lat, lon = resolve_coords(seed, pg)

    # Publish group — always, even if events failed
    publish(
        producer,
        topic=settings.topic_groups_raw,
        value=build_group_raw(seed, group_home, lat, lon, events_scrape_ok).model_dump(mode="json"),
        key=seed.group_urlname,
    )

    # Publish events — only if scrape succeeded
    published = 0
    for event in past:
        er = build_event_raw(seed, event, "past")
        if er:
            publish(producer, topic=settings.topic_events_raw,
                    value=er.model_dump(mode="json"),
                    key=f"{seed.group_urlname}:{er.event_id}")
            published += 1
    for event in upcoming:
        er = build_event_raw(seed, event, "upcoming")
        if er:
            publish(producer, topic=settings.topic_events_raw,
                    value=er.model_dump(mode="json"),
                    key=f"{seed.group_urlname}:{er.event_id}")
            published += 1

    log.info("  → Published 1 group + %d events for %s", published, seed.group_urlname)

    # Write telemetry to scrape_log
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
                published,
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
    log.info("Worker started. Listening on '%s'…", settings.topic_groups_to_scrape)

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 3  # 3 × 5s = 15s of silence = topic is drained

    with psycopg.connect(settings.postgres_uri) as pg:
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                while True:
                    msg = consumer.poll(timeout=5.0)
                    if msg is None:
                        if drain_mode:
                            empty_polls += 1
                            log.info("No messages (%d/%d)…", empty_polls, empty_polls_needed)
                            if empty_polls >= empty_polls_needed:
                                log.info("Topic drained — exiting.")
                                break
                        continue
                    empty_polls = 0  # reset on any message
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
                log.info("Shutting down…")
            finally:
                consumer.close()


def main() -> None:
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()