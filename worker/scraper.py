"""
worker/scraper.py
─────────────────
Reads GroupSeed records from a flat file (one URL per line), scrapes each
group via the appropriate platform handler, and upserts GroupRaw + VenueRaw +
EventRaw records directly into Postgres.

Replaces the previous Kafka-based pipeline. No Kafka dependency.

Usage:
    python -m worker.scraper                          # scrape community/groups.txt
    python -m worker.scraper --input path/to/urls.txt
    python -m worker.scraper --limit 100              # stop after N groups
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import httpx

from shared.db import connect, write_result
from shared.models import GroupSeed
from shared.settings import Settings
from worker.platforms import PLATFORMS, get_platform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

WORKER_ID = os.environ.get("WORKER_ID", __import__("socket").gethostname())
CHECKPOINT_FILE = Path(".scraper-checkpoint.json")


def load_urls(path: Path) -> list[str]:
    urls = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and any(p.can_handle(line) for p in PLATFORMS):
            urls.append(line)
    return urls


def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        return set(json.loads(CHECKPOINT_FILE.read_text()))
    return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.write_text(json.dumps(list(done)))


def url_to_seed(url: str, settings: Settings) -> GroupSeed:
    from datetime import datetime, timezone
    if "lu.ma" in url or "luma.com" in url:
        urlname = url.rstrip("/").split("/")[-1]
        platform = "luma"
    else:
        urlname = url.rstrip("/").split("meetup.com/")[-1].split("/")[0]
        platform = "meetup"
    return GroupSeed(
        group_urlname=urlname,
        group_url=url,
        pro_network=platform,
        seeded_at=datetime.now(timezone.utc),
        platform=platform,
    )


async def scrape_one(
    url: str,
    settings: Settings,
    http_client: httpx.AsyncClient,
) -> None:
    seed = url_to_seed(url, settings)
    try:
        result = await get_platform(url).scrape(
            seed=seed,
            http_client=http_client,
            max_past_events=settings.max_events_per_group,
            worker_id=WORKER_ID,
        )
        return result
    except Exception as exc:
        log.error("Failed to scrape %s: %s", seed.group_urlname, exc)
        return None


async def run(input_path: Path, settings: Settings, limit: int | None) -> None:
    urls = load_urls(input_path)
    done = load_checkpoint()

    pending = [u for u in urls if u not in done]
    if limit:
        pending = pending[:limit]

    log.info(
        "Loaded %d URLs, %d already done, %d to scrape",
        len(urls), len(done), len(pending),
    )

    conn = connect(settings)

    async with httpx.AsyncClient(timeout=30) as http_client:
        for i, url in enumerate(pending, 1):
            log.info("[%d/%d] %s", i, len(pending), url)
            result = await scrape_one(url, settings, http_client)
            if result:
                write_result(conn, result)
                done.add(url)
                save_checkpoint(done)
                log.info(
                    "  -> %s | %d events | %d venues",
                    result.group.name,
                    len(result.past_events) + len(result.upcoming_events),
                    len(result.venues),
                )
            await asyncio.sleep(settings.request_delay_seconds)

    log.info("Done. Scraped %d groups.", len(pending))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Meetup groups into Postgres")
    parser.add_argument(
        "--input", default="community/groups.txt",
        help="File of group URLs to scrape (default: community/groups.txt)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Stop after N groups (useful for testing)",
    )
    args = parser.parse_args()

    settings = Settings()
    try:
        asyncio.run(run(Path(args.input), settings, args.limit))
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
