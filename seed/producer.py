"""
seed/producer.py
────────────────
Lightweight seed producer: fetches group lists from one or more Meetup Pro
networks and publishes one GroupSeed per group.

Networks can be specified via:
  - PRO_NETWORKS_STR env var (space/comma separated) for explicit list
  - PRO_NETWORKS_STR=ALL to scrape all known networks from seed/networks.py

Usage:
    python -m seed.producer
    PRO_NETWORKS_STR=ALL python -m seed.producer
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


SITEMAP_URL = "https://www.meetup.com/sw_pro_1.xml.gz"


async def fetch_all_networks(client: httpx.AsyncClient) -> list[str]:
    """Fetch all pro network slugs from the Meetup sitemap."""
    import gzip, re
    resp = await client.get(
        SITEMAP_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; meetupmap-seed/0.1)"},
    )
    resp.raise_for_status()
    content = gzip.decompress(resp.content).decode()
    networks = re.findall(r'meetup\.com/pro/([^/<]+)/', content)
    unique = sorted(set(networks))
    log.info("Fetched %d pro networks from sitemap", len(unique))
    return unique


async def fetch_groups(network: str, client: httpx.AsyncClient) -> list[dict]:
    """Single API call — returns all groups for a pro network."""
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

    # Check the network actually exists
    if not data.get("data", {}).get("proNetwork"):
        raise ValueError(f"No proNetwork found for '{network}'")

    return (data.get("data", {})
                .get("proNetwork", {})
                .get("groupsSearch", {})
                .get("edges", []))


async def seed_network(
    network: str,
    settings: Settings,
    producer,
    seen_urlnames: set[str],
) -> int:
    """
    Seed one network. Returns number of new groups published.
    seen_urlnames is shared across networks to avoid publishing duplicates
    (a group can belong to multiple pro networks).
    """
    now = datetime.now(timezone.utc)
    published = 0

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            edges = await fetch_groups(network, client)
        except Exception as exc:
            log.warning("[%s] Skipping — %s", network, exc)
            return 0

    for edge in edges:
        node = edge.get("node", {})
        urlname = node.get("link", "").rstrip("/").split("/")[-1]
        if not urlname:
            continue

        # Skip if already published by a previous network in this run
        if urlname.lower() in seen_urlnames:
            log.debug("[%s] Skipping duplicate: %s", network, urlname)
            continue
        seen_urlnames.add(urlname.lower())

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
        )
        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        published += 1

    log.info("[%s] Seeded %d groups (%d total unique so far)",
             network, published, len(seen_urlnames))
    return published


async def run(settings: Settings) -> None:
    log.info("Ensuring Kafka topics exist…")
    ensure_topics(settings, topics=[
        settings.topic_groups_to_scrape,
        settings.topic_groups_raw,
        settings.topic_events_raw,
    ], num_partitions=20)

    producer = make_producer(settings)

    # Resolve which networks to scrape
    async with httpx.AsyncClient(timeout=60) as sitemap_client:
        if settings.pro_networks_str.upper() == "ALL":
            networks = await fetch_all_networks(sitemap_client)
            log.info("Scraping ALL %d networks from sitemap", len(networks))
        else:
            networks = settings.pro_networks
            log.info("Scraping %d networks: %s", len(networks), networks)

    seen_urlnames: set[str] = set()
    total = 0

    for network in networks:
        count = await seed_network(network, settings, producer, seen_urlnames)
        total += count

    producer.flush(timeout=30)
    log.info("Seed complete. %d unique groups published across %d networks.",
             total, len(networks))


def main() -> None:
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()