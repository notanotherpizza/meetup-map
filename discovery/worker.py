"""
discovery/worker.py
───────────────────
Discovery worker: consumes DiscoveryTask messages from Kafka, searches
Meetup/Luma APIs for matching groups, and publishes GroupSeed messages
to the groups-to-scrape topic.

Deduplicates against Postgres groups table to avoid re-seeding known groups.

Usage:
    python -m discovery.worker
"""
import asyncio
import json
import logging
import os
import re
import socket
import sys
from datetime import datetime, timezone

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.kafka_client import make_consumer, make_producer, publish
from shared.models import DiscoveryTask, GroupSeed
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── Meetup GQL ────────────────────────────────────────────────────────────────

MEETUP_GQL_URL = "https://www.meetup.com/gql2"

EVENT_SEARCH_QUERY = """
query eventSearch($filter: EventSearchFilter!, $first: Int, $after: String) {
  eventSearch(filter: $filter, first: $first, after: $after) {
    totalCount
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        group { urlname name city country }
      }
    }
  }
}
"""

GQL_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetup-map-discovery/1.0)",
}

PAGE_SIZE = 20
MAX_PAGES = 10
REQUEST_DELAY_S = 0.5
RATE_LIMIT_SLEEP_S = 30


async def search_meetup(
    client: httpx.AsyncClient,
    keyword: str,
    lat: float,
    lon: float,
    radius: int,
) -> list[dict]:
    """Search Meetup for groups matching keyword near lat/lon."""
    all_groups: dict[str, dict] = {}
    cursor = None
    pages = 0

    while pages < MAX_PAGES:
        payload = {
            "operationName": "eventSearch",
            "variables": {
                "filter": {
                    "query": keyword,
                    "lat": lat,
                    "lon": lon,
                    "radius": radius,
                },
                "first": PAGE_SIZE,
            },
            "query": EVENT_SEARCH_QUERY,
        }
        if cursor:
            payload["variables"]["after"] = cursor

        try:
            resp = await client.post(
                MEETUP_GQL_URL, json=payload, headers=GQL_HEADERS, timeout=20
            )
            if resp.status_code == 429:
                log.warning("Rate limited — sleeping %ds", RATE_LIMIT_SLEEP_S)
                await asyncio.sleep(RATE_LIMIT_SLEEP_S)
                continue

            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                log.warning("GQL errors: %s", data["errors"])
                break

            result = data.get("data", {}).get("eventSearch")
            if not result:
                break

            for edge in result.get("edges", []):
                group = edge.get("node", {}).get("group") or {}
                urlname = group.get("urlname")
                if urlname and urlname not in all_groups:
                    all_groups[urlname] = {
                        "urlname": urlname,
                        "name": group.get("name"),
                        "city": group.get("city"),
                        "country": group.get("country"),
                        "url": f"https://www.meetup.com/{urlname}/",
                        "platform": "meetup",
                    }

            page_info = result.get("pageInfo", {})
            if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
                break

            cursor = page_info["endCursor"]
            pages += 1
            await asyncio.sleep(REQUEST_DELAY_S)

        except httpx.TimeoutException:
            log.warning("Timeout searching Meetup for %r", keyword)
            break
        except Exception as e:
            log.error("Error searching Meetup: %s", e)
            break

    return list(all_groups.values())


# ── Luma Scraping ─────────────────────────────────────────────────────────────

LUMA_BASE_URL = "https://lu.ma"
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)


def _extract_next_data(html: str) -> dict:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def _walk_for_calendars(obj, slugs: set[str]) -> None:
    if isinstance(obj, dict):
        if "slug" in obj and isinstance(obj.get("slug"), str):
            if obj.get("type") in ("calendar", "community") or "calendar" in str(obj.keys()):
                slugs.add(obj["slug"])
        if "calendar" in obj and isinstance(obj["calendar"], dict):
            cal = obj["calendar"]
            slug = cal.get("slug") or cal.get("url")
            if slug and isinstance(slug, str):
                slugs.add(slug)
        for v in obj.values():
            _walk_for_calendars(v, slugs)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_calendars(item, slugs)


async def search_luma(
    client: httpx.AsyncClient,
    slug: str,
) -> list[dict]:
    """Fetch a Luma city/category page and extract calendar slugs."""
    try:
        resp = await client.get(
            f"{LUMA_BASE_URL}/{slug}",
            timeout=20,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; meetup-map-discovery/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        if resp.status_code != 200:
            log.warning("Luma %s returned %d", slug, resp.status_code)
            return []

        html = resp.text
        next_data = _extract_next_data(html)
        if not next_data:
            return []

        slugs: set[str] = set()
        _walk_for_calendars(next_data, slugs)

        # Filter out seed slugs and event IDs
        event_id_re = re.compile(r'^evt-[a-zA-Z0-9]+$')
        valid_slugs = [
            s for s in slugs
            if s and len(s) >= 2 and not event_id_re.match(s) and s != slug
        ]

        return [
            {
                "urlname": s,
                "url": f"https://lu.ma/{s}",
                "platform": "luma",
            }
            for s in valid_slugs
        ]

    except Exception as e:
        log.error("Error fetching Luma %s: %s", slug, e)
        return []


# ── Deduplication ─────────────────────────────────────────────────────────────

def get_existing_groups(conn: psycopg.Connection) -> set[str]:
    """Get set of existing group urlnames (lowercase) from Postgres."""
    with conn.cursor() as cur:
        cur.execute("SELECT LOWER(id) FROM groups")
        return {row["lower"] for row in cur.fetchall()}


# ── Task Processing ───────────────────────────────────────────────────────────

async def process_task(
    task: DiscoveryTask,
    producer,
    settings: Settings,
    http_client: httpx.AsyncClient,
    existing_groups: set[str],
    worker_id: str,
) -> tuple[int, int]:
    """Process a discovery task. Returns (groups_found, groups_new)."""
    now = datetime.now(timezone.utc)
    groups = []

    if task.platform == "meetup" and task.lat and task.lon:
        groups = await search_meetup(
            http_client,
            task.topic,
            task.lat,
            task.lon,
            task.radius_miles or 30,
        )
    elif task.platform == "luma" and task.luma_slug:
        groups = await search_luma(http_client, task.luma_slug)

    new_count = 0
    for group in groups:
        urlname = group["urlname"]
        if urlname.lower() in existing_groups:
            continue

        existing_groups.add(urlname.lower())

        pro_network = f"discovered_{task.platform}"

        seed = GroupSeed(
            group_urlname=urlname,
            group_url=group["url"],
            pro_network=pro_network,
            platform=group["platform"],
            seeded_at=now,
            name=group.get("name"),
            city=group.get("city"),
            country=group.get("country"),
        )

        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        new_count += 1

    return len(groups), new_count


# ── Logging to Postgres ───────────────────────────────────────────────────────

def log_discovery(
    conn: psycopg.Connection,
    task: DiscoveryTask,
    groups_found: int,
    groups_new: int,
    worker_id: str,
    duration_ms: int,
    error: str | None = None,
) -> None:
    """Log discovery task result to Postgres."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO discovery_log
                (task_id, platform, topic, region, groups_found, groups_new,
                 worker_id, duration_ms, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (task_id) DO UPDATE SET
                groups_found = EXCLUDED.groups_found,
                groups_new = EXCLUDED.groups_new,
                worker_id = EXCLUDED.worker_id,
                duration_ms = EXCLUDED.duration_ms,
                error = EXCLUDED.error,
                discovered_at = now()
            """,
            (
                task.task_id, task.platform, task.topic, task.region,
                groups_found, groups_new, worker_id, duration_ms, error,
            ),
        )
    conn.commit()


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run(settings: Settings) -> None:
    consumer = make_consumer(
        settings,
        group_id="meetupmap-discovery",
        topics=[settings.topic_discovery_tasks],
    )
    producer = make_producer(settings)

    worker_id = os.environ.get("WORKER_ID", socket.gethostname()[:12])
    log.info(
        "Discovery worker %s started. Listening on '%s'...",
        worker_id, settings.topic_discovery_tasks,
    )

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 6

    tasks_processed = 0
    groups_discovered = 0

    with psycopg.connect(settings.postgres_uri, row_factory=dict_row) as conn:
        existing_groups = get_existing_groups(conn)
        log.info("Loaded %d existing groups from Postgres", len(existing_groups))

        async def process_loop():
            nonlocal tasks_processed, groups_discovered, empty_polls

            async with httpx.AsyncClient() as http_client:
                while True:
                    msg = consumer.poll(timeout=5.0)

                    if msg is None:
                        if drain_mode:
                            empty_polls += 1
                            log.info(
                                "No messages (%d/%d)... tasks: %d, groups: %d",
                                empty_polls, empty_polls_needed,
                                tasks_processed, groups_discovered,
                            )
                            if empty_polls >= empty_polls_needed:
                                log.info("Topic drained — exiting.")
                                break
                        continue

                    if msg.error():
                        log.error("Kafka error: %s", msg.error())
                        continue

                    empty_polls = 0

                    try:
                        payload = json.loads(msg.value())
                        task = DiscoveryTask(**payload)

                        start_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                        found, new = await process_task(
                            task, producer, settings, http_client,
                            existing_groups, worker_id,
                        )

                        duration_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms

                        log_discovery(conn, task, found, new, worker_id, duration_ms)

                        tasks_processed += 1
                        groups_discovered += new

                        log.info(
                            "[%s] %s/%s: found %d, %d new (total: %d tasks, %d groups)",
                            task.platform, task.topic, task.region,
                            found, new, tasks_processed, groups_discovered,
                        )

                        consumer.commit(msg)
                        producer.flush(timeout=10)

                        await asyncio.sleep(REQUEST_DELAY_S)

                    except Exception as e:
                        log.error("Failed to process task: %s", e, exc_info=True)

        try:
            asyncio.run(process_loop())
        except KeyboardInterrupt:
            log.info(
                "Shutting down. Processed %d tasks, discovered %d groups.",
                tasks_processed, groups_discovered,
            )
        finally:
            consumer.close()


def main() -> None:
    settings = Settings()
    try:
        run(settings)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
