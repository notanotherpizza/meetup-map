"""
seed/producer.py
────────────────
Lightweight seed producer: fetches the group list from the Meetup Pro
network and publishes one GroupSeed per group. That's it.

All per-group enrichment (events, member counts, geocoding) is the
worker's responsibility — workers scale horizontally, the producer doesn't.

Usage:
    python -m seed.producer
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone

import httpx

from shared.kafka_client import ensure_topics, make_producer, publish
from shared.models import GroupSeed
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

MEETUP_GQL_URL = "https://www.meetup.com/gql2"
GEO_HASH = "08215939115765485ea3c349d1b041f5584a07c0fba497583b8c4f123b468d0a"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-seed/0.1)",
}


async def fetch_groups(network: str, client: httpx.AsyncClient) -> list[dict]:
    """Single API call — returns all groups with name, city, country, lat, lon."""
    resp = await client.post(
        MEETUP_GQL_URL,
        json={
            "operationName": "getProNetworkGroupsGeoByUrlname",
            "variables": {"urlname": network, "first": 500, "active": True},
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": GEO_HASH}},
        },
        headers=HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise ValueError(f"GQL errors: {data['errors']}")
    return (data.get("data", {}).get("proNetwork", {})
                .get("groupsSearch", {}).get("edges", []))


async def seed_network(network: str, settings: Settings, producer) -> int:
    now = datetime.now(timezone.utc)
    published = 0

    async with httpx.AsyncClient(timeout=30) as client:
        edges = await fetch_groups(network, client)

    for edge in edges:
        node = edge.get("node", {})
        urlname = node.get("link", "").rstrip("/").split("/")[-1]
        if not urlname:
            continue

        seed = GroupSeed(
            group_urlname=urlname,
            group_url=node.get("link", f"https://www.meetup.com/{urlname}/"),
            pro_network=network,
            seeded_at=now,
            name=node.get("name"),
            city=node.get("city"),
            country=node.get("country"),
            lat=node.get("lat"),
            lon=node.get("lon"),
            # member_count not available from geo query — worker fetches this
        )
        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        published += 1
        log.info("[%s] Seeded: %s (%d so far)", network, urlname, published)

    return published


async def run(settings: Settings) -> None:
    log.info("Ensuring Kafka topics exist…")
    ensure_topics(settings, topics=[
        settings.topic_groups_to_scrape,
        settings.topic_groups_raw,
        settings.topic_events_raw,
    ], num_partitions=2)

    producer = make_producer(settings)

    total = 0
    for network in settings.pro_networks:
        count = await seed_network(network, settings, producer)
        total += count

    producer.flush(timeout=30)
    log.info("Seed complete. %d groups published.", total)


def main() -> None:
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()