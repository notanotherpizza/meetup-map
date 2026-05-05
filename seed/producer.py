"""
seed/producer.py
────────────────
Lightweight seed producer: fetches group lists from one or more Meetup Pro
networks and publishes one GroupSeed per group.

Networks can be specified via:
  - PRO_NETWORKS_STR env var (space/comma separated) for explicit list
  - PRO_NETWORKS_STR=ALL     scrape all known networks + community + discovered
  - PRO_NETWORKS_STR=COMMUNITY  only community/groups.txt
  - PRO_NETWORKS_STR=DISCOVERED emit discovery tasks to Kafka for workers

Group sources:
  - Pro Networks: fetched from Meetup sitemap + GQL API
  - Community: manually curated in community/groups.txt
  - Discovered: read from Postgres (pro_network = 'discovered_meetup' or 'discovered_luma')

Usage:
    python -m seed.producer
    PRO_NETWORKS_STR=ALL python -m seed.producer
    PRO_NETWORKS_STR=COMMUNITY python -m seed.producer
    PRO_NETWORKS_STR=DISCOVERED python -m seed.producer
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from shared.kafka_client import ensure_topics, make_producer, publish
from shared.models import DiscoveryTask, GroupSeed
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

COMMUNITY_DIR = Path(__file__).parent.parent / "community"
COMMUNITY_GROUPS_FILE = COMMUNITY_DIR / "groups.txt"

# Priority ranking for pro_network tags.
# Real Pro Network slugs (anything not listed here) rank highest at 100.
# When upserting a group, don't downgrade its network tag to a lower-priority one.
PRO_NETWORK_PRIORITY = {
    "discovered_meetup": 5,
    "discovered_luma":   5,
    "community":         10,
    # real pro network slugs: 100 (default)
}


def pro_network_rank(tag: str) -> int:
    return PRO_NETWORK_PRIORITY.get(tag, 100)


# ── File loading ──────────────────────────────────────────────────────────────

def _parse_group_line(line: str) -> dict | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    if "meetup.com/" in line:
        urlname = line.rstrip("/").split("/")[-1]
        if urlname:
            return {
                "urlname": urlname,
                "url": line if line.startswith("http") else f"https://www.meetup.com/{urlname}/",
                "platform": "meetup",
            }

    elif "lu.ma/" in line or "luma.com/" in line:
        url = line.replace("luma.com/", "lu.ma/")
        if not url.startswith("http"):
            url = f"https://{url}"
        urlname = url.rstrip("/").split("/")[-1]
        if urlname:
            return {"urlname": urlname, "url": url, "platform": "luma"}

    return None


def _load_groups_file(path: Path, label: str) -> list[dict]:
    if not path.exists():
        log.info("No %s found — skipping", path)
        return []

    groups = []
    unrecognised = 0
    for line in path.read_text().splitlines():
        group = _parse_group_line(line)
        if group:
            groups.append(group)
        elif line.strip() and not line.strip().startswith("#"):
            log.warning("Unrecognised URL format in %s: %s", label, line)
            unrecognised += 1

    log.info("Loaded %d groups from %s (%d unrecognised)", len(groups), label, unrecognised)
    return groups


def load_community_groups() -> list[dict]:
    return _load_groups_file(COMMUNITY_GROUPS_FILE, "community/groups.txt")


def load_discovered_groups_from_postgres(postgres_uri: str) -> dict[str, list[dict]]:
    """Load discovered groups directly from Postgres."""
    import psycopg
    from psycopg.rows import dict_row

    result = {
        "discovered_meetup": [],
        "discovered_luma": [],
    }

    try:
        with psycopg.connect(postgres_uri, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, url, pro_network
                    FROM groups
                    WHERE pro_network IN ('discovered_meetup', 'discovered_luma')
                """)
                for row in cur.fetchall():
                    group_id = row["id"]
                    url = row["url"]
                    pro_network = row["pro_network"]

                    if pro_network == "discovered_meetup":
                        platform = "meetup"
                        if not url:
                            url = f"https://www.meetup.com/{group_id}/"
                    else:
                        platform = "luma"
                        if not url:
                            url = f"https://lu.ma/{group_id}"

                    result[pro_network].append({
                        "urlname": group_id,
                        "url": url,
                        "platform": platform,
                    })

        for tag, groups in result.items():
            log.info("Loaded %d %s groups from Postgres", len(groups), tag)

    except Exception as e:
        log.warning("Could not load discovered groups from Postgres: %s", e)

    return result


# ── Seeding helpers ───────────────────────────────────────────────────────────

def _seed_group_list(
    groups: list[dict],
    pro_network: str,
    settings: Settings,
    producer,
    seen_urlnames: set[str],
) -> int:
    now = datetime.now(timezone.utc)
    published = 0

    for group in groups:
        urlname = group["urlname"]

        if urlname.lower() in seen_urlnames:
            log.debug("[%s] Skipping duplicate: %s", pro_network, urlname)
            continue
        seen_urlnames.add(urlname.lower())

        seed = GroupSeed(
            group_urlname=urlname,
            group_url=group["url"],
            pro_network=pro_network,
            platform=group["platform"],
            seeded_at=now,
        )
        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        published += 1

    log.info("[%s] Seeded %d new groups", pro_network, published)
    return published


def seed_community_groups(
    community_groups: list[dict],
    settings: Settings,
    producer,
    seen_urlnames: set[str],
) -> int:
    return _seed_group_list(
        community_groups, "community", settings, producer, seen_urlnames
    )


def seed_discovered_groups(
    settings: Settings,
    producer,
    seen_urlnames: set[str],
) -> int:
    """Seed discovered groups from Postgres."""
    if not settings.postgres_uri:
        log.warning("No POSTGRES_URI — skipping discovered groups")
        return 0

    discovered = load_discovered_groups_from_postgres(settings.postgres_uri)
    total = 0
    for pro_network, groups in discovered.items():
        if groups:
            total += _seed_group_list(
                groups, pro_network, settings, producer, seen_urlnames
            )
        else:
            log.info("[%s] No groups to seed", pro_network)
    return total


# ── Meetup Pro Network fetching ───────────────────────────────────────────────

async def fetch_all_networks(client: httpx.AsyncClient) -> list[str]:
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


# ── Run tracking ──────────────────────────────────────────────────────────────

async def create_run(settings: Settings, mode: str) -> int | None:
    """
    Create a scrape_runs row and return the run_id.
    Stores the mode string ("ALL", "COMMUNITY", "DISCOVERED", or network names)
    — never the full list of group slugs.
    """
    try:
        import psycopg
        with psycopg.connect(settings.postgres_uri) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "INSERT INTO scrape_runs (networks) VALUES (%s) RETURNING id",
                    (mode,)
                )
                run_id = cur.fetchone()[0]
            pg.commit()
        log.info("Created scrape run #%d (mode=%s)", run_id, mode)
        return run_id
    except Exception as e:
        log.warning("Could not create scrape run: %s", e)
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(settings: Settings) -> None:
    log.info("Ensuring Kafka topics exist...")
    ensure_topics(settings, topics=[
        settings.topic_discovery_tasks,
        settings.topic_groups_to_scrape,
        settings.topic_groups_raw,
        settings.topic_events_raw,
        settings.topic_venues_raw,
    ], num_partitions=20)

    producer = make_producer(settings)
    seen_urlnames: set[str] = set()
    total = 0

    networks_input = settings.pro_networks_str.upper()

    # ------------------------------------------------------------------ #
    # COMMUNITY mode                                                       #
    # ------------------------------------------------------------------ #
    if networks_input == "COMMUNITY":
        log.info("Community-only mode")
        await create_run(settings, "COMMUNITY")
        community_groups = load_community_groups()
        if community_groups:
            total = seed_community_groups(community_groups, settings, producer, seen_urlnames)
        else:
            log.warning("No community groups found in community/groups.txt")
        producer.flush(timeout=30)
        log.info("Community seed complete. %d groups published.", total)
        return

    # ------------------------------------------------------------------ #
    # DISCOVERED mode — emit discovery tasks to Kafka                      #
    # ------------------------------------------------------------------ #
    if networks_input == "DISCOVERED":
        log.info("Discovery mode — emitting discovery tasks")
        await create_run(settings, "DISCOVERED")

        from community.discovery_config import TOPICS, REGIONS, LUMA_CATEGORIES, get_luma_city_slugs

        now = datetime.now(timezone.utc)

        # Meetup discovery tasks: topic × region
        meetup_tasks = 0
        for topic in TOPICS:
            for label, lat, lon, radius, luma_slug in REGIONS:
                task = DiscoveryTask(
                    task_id=f"meetup:{topic}:{label}",
                    platform="meetup",
                    topic=topic,
                    region=label,
                    lat=lat,
                    lon=lon,
                    radius_miles=radius,
                    created_at=now,
                )
                publish(
                    producer,
                    topic=settings.topic_discovery_tasks,
                    value=task.model_dump(mode="json"),
                    key=task.task_id,
                )
                meetup_tasks += 1

        # Luma discovery tasks: city slugs + categories
        luma_tasks = 0
        luma_slugs = get_luma_city_slugs() + LUMA_CATEGORIES
        for slug in luma_slugs:
            task = DiscoveryTask(
                task_id=f"luma:city:{slug}",
                platform="luma",
                topic=slug,
                region=slug,
                luma_slug=slug,
                created_at=now,
            )
            publish(
                producer,
                topic=settings.topic_discovery_tasks,
                value=task.model_dump(mode="json"),
                key=task.task_id,
            )
            luma_tasks += 1

        producer.flush(timeout=30)
        log.info(
            "Discovery tasks published: %d Meetup + %d Luma = %d total",
            meetup_tasks, luma_tasks, meetup_tasks + luma_tasks,
        )
        return

    # ------------------------------------------------------------------ #
    # ALL / explicit network list                                          #
    # ------------------------------------------------------------------ #
    async with httpx.AsyncClient(timeout=60) as sitemap_client:
        if networks_input == "ALL":
            networks = await fetch_all_networks(sitemap_client)
            mode = "ALL"
            log.info("Scraping ALL %d networks from sitemap", len(networks))
        else:
            networks = settings.pro_networks
            mode = " ".join(networks)
            log.info("Scraping %d networks: %s", len(networks), networks)

    run_id = await create_run(settings, mode)
    if run_id:
        log.info("Run ID: %d — set RUN_ID=%d in worker env to track progress",
                 run_id, run_id)

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

    community_groups = load_community_groups()
    if community_groups:
        total += seed_community_groups(community_groups, settings, producer, seen_urlnames)

    total += seed_discovered_groups(settings, producer, seen_urlnames)

    producer.flush(timeout=30)
    log.info(
        "Seed complete. %d unique groups published across %d networks + community + discovered.",
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