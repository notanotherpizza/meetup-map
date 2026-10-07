"""
batch_worker/run.py
────────────────────
Daily batch runner. Orchestrates:
  1. Discovery  — find new Meetup Pro groups (keyword × city grid) and Luma
                  calendars (city/category pages)
  2. Merge      — append newly discovered URLs to community/groups.txt
  3. Scrape     — scrape all groups (Meetup or Luma, dispatched by URL) and
                  upsert into Postgres

Designed to run as a daily cron job on Aiven Apps. Safe to re-run: discovery
deduplicates against existing URLs, and the scraper checkpoints progress so an
interrupted run resumes where it left off.

Usage:
    python -m batch_worker.run
    python -m batch_worker.run --discover-only   # just run discovery, don't scrape
    python -m batch_worker.run --scrape-only     # skip discovery, just scrape
    python -m batch_worker.run --limit 50        # scrape at most N groups (for testing)
"""
import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from community.discover_meetup import discover as run_discovery_meetup
from community.discover_luma import discover as run_discovery_luma
from shared.db import connect, write_result
from shared.models import GroupSeed
from shared.settings import Settings
from worker.scraper import load_urls, url_to_seed, WORKER_ID
from worker.platforms import get_platform, PLATFORMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

GROUPS_FILE = Path("community/groups.txt")
DISCOVERY_OUTPUT_MEETUP = Path("community/discovered_meetup.txt")
DISCOVERY_OUTPUT_LUMA = Path("community/discovered_luma.txt")


# ── Step 1: Discovery ──────────────────────────────────────────────────────────

def _read_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        l.strip() for l in path.read_text().splitlines()
        if l.strip() and not l.startswith("#")
    }


async def run_discover() -> int:
    """Run Meetup + Luma discovery and append new groups to their discovered files.
    Returns count of newly discovered groups across both platforms."""
    log.info("=== Step 1: Discovery ===")
    before_meetup = _read_urls(DISCOVERY_OUTPUT_MEETUP)
    before_luma = _read_urls(DISCOVERY_OUTPUT_LUMA)

    await run_discovery_meetup(dry_run=False, output_path=DISCOVERY_OUTPUT_MEETUP)
    await run_discovery_luma(dry_run=False, output_path=DISCOVERY_OUTPUT_LUMA)

    new_count = (
        len(_read_urls(DISCOVERY_OUTPUT_MEETUP) - before_meetup)
        + len(_read_urls(DISCOVERY_OUTPUT_LUMA) - before_luma)
    )
    log.info("Discovery complete — %d new groups found", new_count)
    return new_count


# ── Step 2: Merge ─────────────────────────────────────────────────────────────

def merge_discovered() -> int:
    """Merge discovered URLs (Meetup + Luma) into groups.txt. Returns count of net-new URLs added."""
    log.info("=== Step 2: Merge ===")
    existing = _read_urls(GROUPS_FILE)
    discovered = _read_urls(DISCOVERY_OUTPUT_MEETUP) | _read_urls(DISCOVERY_OUTPUT_LUMA)

    new_urls = sorted(discovered - existing)
    if not new_urls:
        log.info("No new URLs to merge")
        return 0

    with GROUPS_FILE.open("a") as f:
        f.write(f"\n# Discovered by daily batch runner\n")
        for url in new_urls:
            f.write(url + "\n")

    log.info("Merged %d new URLs into %s", len(new_urls), GROUPS_FILE)
    return len(new_urls)


# ── Step 3: Scrape ─────────────────────────────────────────────────────────────

def _url_to_urlname(url: str) -> str:
    return url_to_seed(url, None).group_urlname


def fetch_latest_groups(url: str | None) -> list[str] | None:
    """Fetch the current groups.txt so a long-lived container isn't stuck with
    the list baked into its image at build time. Returns None on any failure."""
    if not url:
        return None
    try:
        resp = httpx.get(url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Could not fetch %s (%s) - using bundled groups.txt", url, exc)
        return None
    return [
        l.strip() for l in resp.text.splitlines()
        if l.strip() and not l.startswith("#") and any(p.can_handle(l.strip()) for p in PLATFORMS)
    ]


def order_by_staleness(urls: list[str], conn) -> list[str]:
    """Never-scraped groups first, then oldest scraped_at first.

    Progress lives in Postgres rather than a checkpoint file, so a container
    restart can't send the scraper back over the head of the list while
    newly discovered groups at the tail never get reached."""
    scraped = dict(conn.execute("SELECT group_urlname, scraped_at FROM groups").fetchall())
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    seen: set[str] = set()
    keyed = []
    for i, url in enumerate(urls):
        name = _url_to_urlname(url)
        if name in seen:
            continue
        seen.add(name)
        keyed.append((scraped.get(name, epoch), i, url))
    keyed.sort()
    return [u for _, _, u in keyed]


async def run_scrape(settings: Settings, limit: int | None) -> None:
    log.info("=== Step 3: Scrape ===")
    urls = fetch_latest_groups(os.environ.get("GROUPS_URL")) or load_urls(GROUPS_FILE)
    conn = connect(settings)
    pending = order_by_staleness(urls, conn)
    if limit:
        pending = pending[:limit]

    log.info("Total: %d URLs | Scraping: %d", len(urls), len(pending))

    if not pending:
        log.info("Nothing to scrape.")
        return

    async with httpx.AsyncClient(timeout=30) as http_client:
        for i, url in enumerate(pending, 1):
            log.info("[%d/%d] %s", i, len(pending), url)
            seed = url_to_seed(url, settings)
            try:
                result = await get_platform(url).scrape(
                    seed=seed,
                    http_client=http_client,
                    max_past_events=settings.max_events_per_group,
                    worker_id=WORKER_ID,
                )
                write_result(conn, result)
                log.info(
                    "  -> %s | %d events | %d venues",
                    result.group.name,
                    len(result.past_events) + len(result.upcoming_events),
                    len(result.venues),
                )
            except Exception as exc:
                log.error("  -> Failed: %s", exc)

            await asyncio.sleep(settings.request_delay_seconds)

    log.info("Scrape complete.")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main(discover: bool, scrape: bool, limit: int | None) -> None:
    settings = Settings()

    if discover:
        await run_discover()
        merge_discovered()

    if scrape:
        await run_scrape(settings, limit)


def entrypoint() -> None:
    parser = argparse.ArgumentParser(description="Daily meetup-map batch runner")
    parser.add_argument("--discover-only", action="store_true")
    parser.add_argument("--scrape-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    discover = not args.scrape_only
    scrape = not args.discover_only

    try:
        asyncio.run(main(discover=discover, scrape=scrape, limit=args.limit))
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    entrypoint()
