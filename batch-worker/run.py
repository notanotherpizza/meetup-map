"""
batch-worker/run.py
────────────────────
Daily batch runner. Orchestrates:
  1. Discovery  — find new Meetup Pro groups via keyword × city grid
  2. Merge      — append newly discovered URLs to community/groups.txt
  3. Scrape     — scrape all groups and write to Iceberg on R2

Designed to run as a daily cron job on Aiven Apps. Safe to re-run: discovery
deduplicates against existing URLs, and the scraper checkpoints progress so an
interrupted run resumes where it left off.

Usage:
    python -m batch-worker.run
    python -m batch-worker.run --discover-only   # just run discovery, don't scrape
    python -m batch-worker.run --scrape-only     # skip discovery, just scrape
    python -m batch-worker.run --limit 50        # scrape at most N groups (for testing)
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

import httpx

from community.discover_meetup import discover as run_discovery
from shared.iceberg import make_catalog, get_tables, write_result
from shared.models import GroupSeed
from shared.settings import Settings
from worker.scraper import load_urls, load_checkpoint, save_checkpoint, url_to_seed, WORKER_ID
from worker.platforms.meetup import MeetupPlatform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

GROUPS_FILE = Path("community/groups.txt")
DISCOVERY_OUTPUT = Path("community/discovered_meetup.txt")
PLATFORM = MeetupPlatform()


# ── Step 1: Discovery ──────────────────────────────────────────────────────────

async def run_discover() -> int:
    """Run Meetup discovery and append new groups to discovered_meetup.txt.
    Returns count of newly discovered groups."""
    log.info("=== Step 1: Discovery ===")
    before = set()
    if DISCOVERY_OUTPUT.exists():
        before = {
            l.strip() for l in DISCOVERY_OUTPUT.read_text().splitlines()
            if l.strip() and not l.startswith("#")
        }

    await run_discovery(dry_run=False, output_path=DISCOVERY_OUTPUT)

    after = set()
    if DISCOVERY_OUTPUT.exists():
        after = {
            l.strip() for l in DISCOVERY_OUTPUT.read_text().splitlines()
            if l.strip() and not l.startswith("#")
        }

    new_count = len(after - before)
    log.info("Discovery complete — %d new groups found", new_count)
    return new_count


# ── Step 2: Merge ─────────────────────────────────────────────────────────────

def merge_discovered() -> int:
    """Merge discovered URLs into groups.txt. Returns count of net-new URLs added."""
    log.info("=== Step 2: Merge ===")
    existing = set()
    if GROUPS_FILE.exists():
        existing = {
            l.strip() for l in GROUPS_FILE.read_text().splitlines()
            if l.strip() and not l.startswith("#")
        }

    discovered = set()
    if DISCOVERY_OUTPUT.exists():
        discovered = {
            l.strip() for l in DISCOVERY_OUTPUT.read_text().splitlines()
            if l.strip() and not l.startswith("#")
        }

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

async def run_scrape(settings: Settings, limit: int | None) -> None:
    log.info("=== Step 3: Scrape ===")
    urls = load_urls(GROUPS_FILE)
    done = load_checkpoint()
    pending = [u for u in urls if u not in done]
    if limit:
        pending = pending[:limit]

    log.info(
        "Total: %d URLs | Done: %d | Pending: %d",
        len(urls), len(done), len(pending),
    )

    if not pending:
        log.info("Nothing to scrape.")
        return

    catalog = make_catalog(settings)
    groups_table, events_table, venues_table = get_tables(catalog)

    async with httpx.AsyncClient(timeout=30) as http_client:
        for i, url in enumerate(pending, 1):
            log.info("[%d/%d] %s", i, len(pending), url)
            seed = url_to_seed(url, settings)
            try:
                result = await PLATFORM.scrape(
                    seed=seed,
                    http_client=http_client,
                    max_past_events=settings.max_events_per_group,
                    worker_id=WORKER_ID,
                )
                write_result(result, groups_table, events_table, venues_table)
                done.add(url)
                save_checkpoint(done)
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
