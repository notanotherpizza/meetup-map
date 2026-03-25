"""
seed/producer.py
────────────────
Seed producer: reads every group from one or more Meetup Pro networks
and publishes a GroupSeed message to `groups-to-scrape` for each one.
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

# Persisted query hash for getProNetworkGroupsGeoByUrlname
# Captured from browser network traffic — fetches up to 500 groups in one call
GEO_GROUPS_HASH = "08215939115765485ea3c349d1b041f5584a07c0fba497583b8c4f123b468d0a"


async def fetch_groups(network: str, client: httpx.AsyncClient) -> list[dict]:
    """
    Fetch all groups for a pro network using Meetup's persisted GraphQL query.
    Returns a list of group dicts with urlname, name, lat, lon, city, country.
    """
    resp = await client.post(
        MEETUP_GQL_URL,
        json={
            "operationName": "getProNetworkGroupsGeoByUrlname",
            "variables": {
                "urlname": network,
                "first": 500,
                "active": True,
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": GEO_GROUPS_HASH,
                }
            },
        },
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; meetupmap-seed/0.1)",
        },
    )
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        raise ValueError(f"GraphQL errors: {data['errors']}")

    # Log the top-level keys so we can see the response shape
    log.info("Response keys: %s", list(data.get("data", {}).keys()))

    # Navigate to the groups list — adjust path based on actual response
    network_data = data.get("data", {})
    
    # Try common paths
    for path in [
        ["proNetwork", "groupsSearch", "edges"],
        ["proNetworkByUrlname", "groups", "edges"],
        ["proNetworkByUrlname", "groups"],
    ]:
        obj = network_data
        for key in path:
            obj = obj.get(key, {}) if isinstance(obj, dict) else None
            if obj is None:
                break
        if obj and isinstance(obj, list):
            log.info("Found %d groups at path: %s", len(obj), path)
            return obj

    log.warning("Could not find groups list. Full response: %s", str(data)[:1000])
    return []


async def seed_network(
    network: str,
    settings: Settings,
    producer,
) -> int:
    now = datetime.now(timezone.utc)
    published = 0

    async with httpx.AsyncClient(timeout=30) as client:
        raw_groups = await fetch_groups(network, client)

        for item in raw_groups:
            # Handle both edge-wrapped and direct group objects
            group = item.get("node", item) if isinstance(item, dict) else item

            urlname = group.get("urlname") or group.get("link", "").rstrip("/").split("/")[-1] or group.get("id", "")
            if not urlname:
                continue

            seed = GroupSeed(
                group_urlname=urlname,
                group_url=group.get("link") or f"https://www.meetup.com/{urlname}/",
                pro_network=network,
                seeded_at=now,
            )
            publish(
                producer,
                topic=settings.topic_groups_to_scrape,
                value=seed.model_dump(mode="json"),
                key=urlname,
            )
            published += 1
            log.info("[%s] Seeded: %s (%d so far)", network, urlname, published)

    return published


async def run(settings: Settings) -> None:
    log.info("Ensuring Kafka topics exist…")
    ensure_topics(
        settings,
        topics=[
            settings.topic_groups_to_scrape,
            settings.topic_groups_raw,
            settings.topic_events_raw,
        ],
        num_partitions=2,
    )

    producer = make_producer(settings)

    total = 0
    for network in settings.pro_networks:
        count = await seed_network(network, settings, producer)
        total += count

    log.info("Flushing producer…")
    producer.flush(timeout=30)
    log.info("Seed complete. Total groups published: %d", total)


def main() -> None:
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
