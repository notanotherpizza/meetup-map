"""
worker/scraper.py
─────────────────
Worker: consumes GroupSeed messages from `groups-to-scrape`, fetches group
metadata and events from Meetup's gql2 API, and publishes GroupRaw and
EventRaw messages to their respective topics.

Run one or more workers — they share a consumer group so work is distributed
automatically across however many instances are running.

Usage:
    python -m worker.scraper
"""
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone

import httpx

from shared.kafka_client import make_consumer, make_producer, publish
from shared.models import EventRaw, GroupRaw, GroupSeed
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

MEETUP_GQL_URL = "https://www.meetup.com/gql2"

PAST_EVENTS_HASH     = "321388b1e4a11b17a57efe3ae7a90abfecbc703a4f4e99519772294924c21351"
UPCOMING_EVENTS_HASH = "066e3709c68718d5ce9dd909e979ac70f99835fb3722cef77756ded808d5ca08"
GEO_GROUPS_HASH      = "08215939115765485ea3c349d1b041f5584a07c0fba497583b8c4f123b468d0a"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-worker/0.1)",
}


async def gql(client, operation, variables, hash):
    now = datetime.now(timezone.utc).isoformat()
    resp = await client.post(
        MEETUP_GQL_URL,
        json={
            "operationName": operation,
            "variables": variables,
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": hash}},
        },
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise ValueError(f"GQL errors in {operation}: {data['errors']}")
    return data


async def fetch_events(urlname, client, max_events):
    now = datetime.now(timezone.utc).isoformat()
    past, upcoming = [], []

    cursor = None
    while len(past) < max_events:
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
    edges = (data.get("data", {}).get("groupByUrlname", {})
                 .get("events", {}).get("edges", []))
    upcoming.extend(edge["node"] for edge in edges if "node" in edge)

    return past[:max_events], upcoming


async def fetch_group_meta(urlname, client):
    data = await gql(client, "getProNetworkGroupsGeoByUrlname",
                     {"urlname": "pydata", "first": 500, "active": True}, GEO_GROUPS_HASH)
    edges = (data.get("data", {}).get("proNetwork", {})
                 .get("groupsSearch", {}).get("edges", []))
    for edge in edges:
        node = edge.get("node", {})
        if node.get("link", "").rstrip("/").split("/")[-1].lower() == urlname.lower():
            return node
    return {}


def build_group_raw(seed, meta):
    memberships = meta.get("memberships")
    return GroupRaw(
        group_urlname=seed.group_urlname,
        name=meta.get("name", seed.group_urlname),
        pro_network=seed.pro_network,
        city=meta.get("city"),
        country=meta.get("country"),
        lat=meta.get("lat"),
        lon=meta.get("lon"),
        member_count=memberships.get("count") if isinstance(memberships, dict) else None,
        meetup_url=meta.get("link", seed.group_url),
        scraped_at=datetime.now(timezone.utc),
        scrape_method="gql2",
    )


def build_event_raw(seed, event, status):
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
    is_online = (event.get("isOnline", False)
                 or event.get("venueType") == "ONLINE"
                 or "online" in (venue.get("name") or "").lower())
    going = event.get("going") or {}
    rsvp_count = going.get("totalCount") if isinstance(going, dict) else None
    return EventRaw(
        event_id=event_id,
        group_urlname=seed.group_urlname,
        title=event.get("title", ""),
        description=event.get("description", ""),
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


async def process_seed(seed, producer, settings, client):
    log.info("Processing: %s", seed.group_urlname)
    try:
        past, upcoming = await fetch_events(seed.group_urlname, client, settings.max_events_per_group)
    except Exception as exc:
        log.error("  Events fetch failed for %s: %s", seed.group_urlname, exc)
        past, upcoming = [], []
    log.info("  → %d past, %d upcoming events", len(past), len(upcoming))

    try:
        meta = await fetch_group_meta(seed.group_urlname, client)
    except Exception as exc:
        log.warning("  Meta fetch failed for %s: %s", seed.group_urlname, exc)
        meta = {}

    publish(producer, topic=settings.topic_groups_raw,
            value=build_group_raw(seed, meta).model_dump(mode="json"),
            key=seed.group_urlname)

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


async def run(settings):
    consumer = make_consumer(settings, group_id="meetupmap-workers",
                             topics=[settings.topic_groups_to_scrape])
    producer = make_producer(settings)
    log.info("Worker started. Listening on '%s'…", settings.topic_groups_to_scrape)

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            while True:
                msg = consumer.poll(timeout=5.0)
                if msg is None:
                    continue
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
            log.info("Shutting down…")
        finally:
            consumer.close()


def main():
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()