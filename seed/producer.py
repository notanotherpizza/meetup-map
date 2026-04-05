"""
seed/producer.py
────────────────
Lightweight seed producer: fetches group lists from one or more Meetup Pro
networks and publishes one GroupSeed per group.

Networks can be specified via:
  - PRO_NETWORKS_STR env var (space/comma separated) for explicit list
  - PRO_NETWORKS_STR=ALL to scrape all known networks from seed/networks.py

Community-submitted groups are read from community/groups.txt and seeded
alongside Pro Network groups on every run. Supported platforms:
  - meetup.com  — seeded as platform="meetup"
  - lu.ma       — seeded as platform="luma"

Usage:
    python -m seed.producer
    PRO_NETWORKS_STR=ALL python -m seed.producer
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

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

GEO_QUERY = """
query getProNetworkGroupsGeoByUrlname($urlname: String!, $first: Int, $active: Boolean) {
  proNetwork(urlname: $urlname) {
    groupsSearch(filter: {active: $active}, first: $first) {
      edges {
        node {
          name
          link
          city
          country
          lat
          lon
        }
      }
    }
  }
}
"""

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-seed/0.1)",
}

SITEMAP_URL = "https://www.meetup.com/sw_pro_1.xml.gz"

# Path to community-submitted groups file, relative to repo root
COMMUNITY_GROUPS_FILE = Path(__file__).parent.parent / "community" / "groups.txt"


def load_community_groups() -> list[dict]:
    """
    Read community/groups.txt and return a list of dicts with urlname + platform.
    Lines starting with # or blank lines are ignored.
    Returns [] if the file doesn't exist.
    """
    if not COMMUNITY_GROUPS_FILE.exists():
        log.info("No community/groups.txt found — skipping community groups")
        return []

    groups = []
    for line in COMMUNITY_GROUPS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if "meetup.com/" in line:
            urlname = line.rstrip("/").split("/")[-1]
            if urlname:
                groups.append({
                    "urlname": urlname,
                    "url": line if line.startswith("http") else f"https://www.meetup.com/{urlname}/",
                    "platform": "meetup",
                })
        elif "lu.ma/" in line or "luma.com/" in line:
            # Normalise luma.com → lu.ma
            url = line.replace("luma.com/", "lu.ma/")
            if not url.startswith("http"):
                url = f"https://{url}"
            urlname = url.rstrip("/").split("/")[-1]
            if urlname:
                groups.append({
                    "urlname": urlname,
                    "url": url,
                    "platform": "luma",
                })
        else:
            log.warning("Unrecognised URL format in community/groups.txt: %s", line)

    log.info(
        "Loaded %d community-submitted groups from %s",
        len(groups), COMMUNITY_GROUPS_FILE,
    )
    return groups


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
    """Single API call with APQ retry — returns all groups for a pro network."""
    payload = {
        "operationName": "getProNetworkGroupsGeoByUrlname",
        "variables": {"urlname": network, "first": 500, "active": True},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": GEO_HASH}},
    }

    resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()

    errors = data.get("errors", [])
    if any(e.get("extensions", {}).get("classification") == "PersistedQueryNotFound"
           for e in errors):
        log.debug("[%s] PersistedQueryNotFound — retrying with full query", network)
        payload["query"] = GEO_QUERY
        resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    if "errors" in data:
        raise ValueError(f"GQL errors: {data['errors']}")

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
    Seed one Meetup Pro network. Returns number of new groups published.
    seen_urlnames is shared across networks to avoid publishing duplicates.
    """
    now = datetime.now(timezone.utc)
    published = 0

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10, read=30, write=10, pool=5)
    ) as client:
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

        if urlname.lower() in seen_urlnames:
            log.debug("[%s] Skipping duplicate: %s", network, urlname)
            continue
        seen_urlnames.add(urlname.lower())

        seed = GroupSeed(
            group_urlname=urlname,
            group_url=node.get("link", f"https://www.meetup.com/{urlname}/"),
            pro_network=network,
            platform="meetup",
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


def seed_community_groups(
    community_groups: list[dict],
    settings: Settings,
    producer,
    seen_urlnames: set[str],
) -> int:
    """
    Publish GroupSeed messages for community-submitted groups.
    Skips groups already seen from Pro Network discovery.
    Supports meetup and luma platforms.
    Returns number of new groups published.
    """
    now = datetime.now(timezone.utc)
    published = 0

    for group in community_groups:
        platform = group["platform"]
        urlname = group["urlname"]

        if urlname.lower() in seen_urlnames:
            log.debug("[community] Skipping duplicate: %s", urlname)
            continue
        seen_urlnames.add(urlname.lower())

        seed = GroupSeed(
            group_urlname=urlname,
            group_url=group["url"],
            pro_network="community",
            platform=platform,
            seeded_at=now,
        )
        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        published += 1
        log.info("[community] Seeded %s (platform=%s)", urlname, platform)

    log.info("[community] Seeded %d new groups", published)
    return published


async def create_run(settings: Settings, networks: list[str]) -> int | None:
    """Create a scrape_runs row and return the run_id."""
    try:
        import psycopg
        with psycopg.connect(settings.postgres_uri) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "INSERT INTO scrape_runs (networks) VALUES (%s) RETURNING id",
                    (",".join(networks),)
                )
                run_id = cur.fetchone()[0]
            pg.commit()
        log.info("Created scrape run #%d", run_id)
        return run_id
    except Exception as e:
        log.warning("Could not create scrape run: %s", e)
        return None


async def run(settings: Settings) -> None:
    log.info("Ensuring Kafka topics exist...")
    ensure_topics(settings, topics=[
        settings.topic_groups_to_scrape,
        settings.topic_groups_raw,
        settings.topic_events_raw,
        settings.topic_venues_raw,
    ], num_partitions=20)

    producer = make_producer(settings)

    networks_input = settings.pro_networks_str.upper()

    # COMMUNITY mode: skip Pro Network discovery entirely, only seed community/groups.txt
    if networks_input == "COMMUNITY":
        log.info("Community-only mode — skipping Pro Network discovery")
        community_groups = load_community_groups()
        if community_groups:
            seen_urlnames: set[str] = set()
            total = seed_community_groups(community_groups, settings, producer, seen_urlnames)
        else:
            log.warning("No community groups found in community/groups.txt")
            total = 0
        producer.flush(timeout=30)
        log.info("Community seed complete. %d groups published.", total)
        return

    async with httpx.AsyncClient(timeout=60) as sitemap_client:
        if networks_input == "ALL":
            networks = await fetch_all_networks(sitemap_client)
            log.info("Scraping ALL %d networks from sitemap", len(networks))
        else:
            networks = settings.pro_networks
            log.info("Scraping %d networks: %s", len(networks), networks)

    run_id = await create_run(settings, networks)
    if run_id:
        log.info("Run ID: %d — set RUN_ID=%d in worker env to track progress",
                 run_id, run_id)

    seen_urlnames: set[str] = set()
    total = 0
    sem = asyncio.Semaphore(10)

    async def seed_bounded(network: str) -> int:
        async with sem:
            try:
                return await asyncio.wait_for(
                    seed_network(network, settings, producer, seen_urlnames),
                    timeout=60,
                )
            except asyncio.TimeoutError:
                log.warning("[%s] Timed out after 60s — skipping", network)
                return 0

    results = await asyncio.gather(
        *[seed_bounded(n) for n in networks],
        return_exceptions=True,
    )

    for network, result in zip(networks, results):
        if isinstance(result, Exception):
            log.warning("[%s] Failed: %s", network, result)
        else:
            total += result

    # Seed community-submitted groups after Pro Network discovery
    community_groups = load_community_groups()
    if community_groups:
        total += seed_community_groups(community_groups, settings, producer, seen_urlnames)

    producer.flush(timeout=30)
    log.info(
        "Seed complete. %d unique groups published across %d networks + community.",
        total, len(networks),
    )


def main() -> None:
    settings = Settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()