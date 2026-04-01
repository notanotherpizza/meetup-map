"""
worker/scraper.py
─────────────────
Stateless worker: consumes GroupSeed messages, calls three Meetup gql2
endpoints per group (groupHome, getPastGroupEvents, getUpcomingGroupEvents),
and publishes GroupRaw + VenueRaw + EventRaw messages.

No Postgres dependency — all geocoding and persistence handled by the sink.

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

# ── Meetup API ────────────────────────────────────────────────────────────────

GQL_QUERIES = {
    "groupHome": """
query groupHome($urlname: String!, $includePrivateInfo: Boolean) {
  groupByUrlname(urlname: $urlname) {
    stats {
      memberCounts {
        all
      }
    }
  }
}
""",
    "getPastGroupEvents": """
query getPastGroupEvents($urlname: String!, $beforeDateTime: String, $after: String) {
  groupByUrlname(urlname: $urlname) {
    events(input: {startDateBefore: $beforeDateTime}, after: $after, first: 20) {
      totalCount
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id title eventUrl dateTime isOnline venueType
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
    "getUpcomingGroupEvents": """
query getUpcomingGroupEvents($urlname: String!, $afterDateTime: String) {
  groupByUrlname(urlname: $urlname) {
    events(input: {startDateAfter: $afterDateTime}, first: 20) {
      edges {
        node {
          id title eventUrl dateTime isOnline venueType
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
}

HASHES = {
    "groupHome": GROUP_HOME_HASH,
    "getPastGroupEvents": PAST_EVENTS_HASH,
    "getUpcomingGroupEvents": UPCOMING_EVENTS_HASH,
}


async def gql(client, operation, variables, hash_):
    payload = {
        "operationName": operation,
        "variables": variables,
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": hash_}},
    }
    resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()

    # APQ retry
    errors = data.get("errors", [])
    if any(e.get("extensions", {}).get("classification") == "PersistedQueryNotFound"
           for e in errors):
        log.debug("PersistedQueryNotFound for %s — retrying with full query", operation)
        if operation in GQL_QUERIES:
            payload["query"] = GQL_QUERIES[operation]
            resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
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
    """Returns (past_events, upcoming_events, total_past_count)."""
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


# ── Message builders ──────────────────────────────────────────────────────────

def build_group_raw(
    seed: GroupSeed,
    group_home: dict,
    events_scrape_ok: bool,
    total_past_events: int | None,
    worker_id: str,
    scrape_duration_ms: int,
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
        member_count=member_count,
        meetup_url=seed.group_url,
        scraped_at=datetime.now(timezone.utc),
        scrape_method="gql2",
        events_scrape_ok=events_scrape_ok,
        total_past_events=total_past_events,
        worker_id=worker_id,
        scrape_duration_ms=scrape_duration_ms,
    )


def build_venue_raw(venue: dict, now: datetime) -> VenueRaw | None:
    venue_id = str(venue.get("id", "")).strip()
    if not venue_id:
        return None
    return VenueRaw(
        venue_id=venue_id,
        name=venue.get("name") or None,
        address=venue.get("address") or None,
        city=venue.get("city") or None,
        state=venue.get("state") or None,
        country=venue.get("country") or None,
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
) -> None:
    log.info("Processing: %s", seed.group_urlname)
    t_start = asyncio.get_event_loop().time()
    worker_id = os.environ.get("WORKER_ID", __import__("socket").gethostname())

    events_scrape_ok = False
    past: list[dict] = []
    upcoming: list[dict] = []
    group_home: dict = {}
    total_past_count: int | None = None

    try:
        group_home, (past, upcoming, total_past_count) = await asyncio.gather(
            fetch_group_home(seed.group_urlname, client),
            fetch_events(seed.group_urlname, client, settings.max_events_per_group),
        )
        events_scrape_ok = True
    except Exception as exc:
        log.warning("  Events fetch failed for %s: %s — will still write group record",
                    seed.group_urlname, exc)
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

    now = datetime.now(timezone.utc)
    duration_ms = int((asyncio.get_event_loop().time() - t_start) * 1000)

    # Publish group
    publish(
        producer,
        topic=settings.topic_groups_raw,
        value=build_group_raw(
            seed, group_home, events_scrape_ok, total_past_count,
            worker_id, duration_ms,
        ).model_dump(mode="json"),
        key=seed.group_urlname,
    )

    # Publish venues + events
    published_venues: set[str] = set()
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

        if venue_id and not is_online and venue_id not in published_venues:
            vr = build_venue_raw(venue, now)
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


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def run(settings: Settings) -> None:
    consumer = make_consumer(settings, group_id="meetupmap-workers",
                             topics=[settings.topic_groups_to_scrape])
    producer = make_producer(settings)
    log.info("Worker started. Listening on '%s'...", settings.topic_groups_to_scrape)

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 3

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
                    await process_seed(seed, producer, settings, client)
                    producer.flush(timeout=10)
                    consumer.commit(msg)
                    await asyncio.sleep(settings.request_delay_seconds)
                except Exception as exc:
                    log.error("Failed to process %s: %s", msg.key(), exc, exc_info=True)
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