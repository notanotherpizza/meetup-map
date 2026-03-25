"""
seed/producer.py
────────────────
Seed producer: reads every group from one or more Meetup Pro networks
and publishes a GroupSeed message to `groups-to-scrape` for each one.

Run once to bootstrap, or on a cron to pick up newly created groups.
Already-known groups will be re-queued (workers should upsert, not insert).

Usage:
    python -m seed.producer
    # or after pip install -e .
    meetupmap-seed
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx
from playwright.async_api import async_playwright, Browser, Page

from shared.kafka_client import ensure_topics, make_producer, publish
from shared.models import GroupSeed
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ── Meetup GraphQL ────────────────────────────────────────────────────────────
# Meetup exposes a public GraphQL endpoint. The pro network member list
# is paginated via a cursor. This works without authentication for public
# pro networks like pydata.

MEETUP_GQL_URL = "https://www.meetup.com/gql"

PRO_NETWORK_QUERY = """
query ProNetworkGroups($urlname: String!, $cursor: String) {
  proNetworkByUrlname(urlname: $urlname) {
    groupsSearch(input: { first: 100, after: $cursor }) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
          urlname
          name
          city
          country
          lat
          lon
          memberships {
            count
          }
          link
        }
      }
    }
  }
}
"""


async def fetch_groups_via_api(
    network: str,
    client: httpx.AsyncClient,
    delay: float,
) -> AsyncIterator[dict]:
    """
    Walk the GraphQL pagination cursor and yield raw group dicts.
    Raises httpx.HTTPError on non-2xx — caller decides whether to fallback.
    """
    cursor = None
    page = 0

    while True:
        page += 1
        log.info("[%s] Fetching page %d via API (cursor: %s)", network, page, cursor)

        resp = await client.post(
            MEETUP_GQL_URL,
            json={
                "query": PRO_NETWORK_QUERY,
                "variables": {"urlname": network, "cursor": cursor},
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                # Meetup's GQL endpoint is public but politely identify ourselves
                "User-Agent": "meetupmap-seed/0.1 (https://github.com/yourusername/meetupmap)",
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise ValueError(f"GraphQL errors: {data['errors']}")

        network_data = data.get("data", {}).get("proNetworkByUrlname")
        if not network_data:
            raise ValueError(f"No proNetworkByUrlname in response for '{network}'")

        search = network_data["groupsSearch"]
        edges = search.get("edges", [])

        for edge in edges:
            yield edge["node"]

        page_info = search.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break

        cursor = page_info["endCursor"]
        await asyncio.sleep(delay)


# ── Playwright fallback ───────────────────────────────────────────────────────

async def fetch_groups_via_playwright(
    network: str,
    browser: Browser,
    delay: float,
) -> AsyncIterator[dict]:
    """
    Fallback: use Playwright to scrape the Pro network page.
    Meetup renders group cards in JS, so we wait for them to appear.
    Yields minimal dicts (urlname + link only) — the worker will enrich.
    """
    url = f"https://www.meetup.com/pro/{network}/"
    log.info("[%s] Falling back to Playwright at %s", network, url)

    page: Page = await browser.new_page()
    await page.goto(url, wait_until="networkidle", timeout=60_000)

    # Meetup pro pages list groups as anchor tags with the group URL
    # The selector may need updating if Meetup changes their markup.
    # Inspect meetup.com/pro/pydata/ → look for group card links.
    group_links = await page.eval_on_selector_all(
        "a[href*='meetup.com/']",
        """els => els
            .map(a => a.href)
            .filter(h => /meetup\\.com\\/[\\w-]+\\/?$/.test(h))
            .filter((v, i, arr) => arr.indexOf(v) === i)
        """,
    )

    log.info("[%s] Playwright found %d candidate links", network, len(group_links))

    for link in group_links:
        # Extract urlname from URL: https://www.meetup.com/pydata-london/ → pydata-london
        urlname = link.rstrip("/").split("/")[-1]
        if urlname in ("pro", network, "meetup.com", ""):
            continue
        yield {"urlname": urlname, "link": link}
        await asyncio.sleep(delay)

    await page.close()


# ── Core logic ────────────────────────────────────────────────────────────────

async def seed_network(
    network: str,
    settings: Settings,
    producer,
    browser: Browser | None,
) -> int:
    """
    Fetch all groups for one pro network and publish GroupSeed messages.
    Returns the number of groups published.
    """
    published = 0
    now = datetime.now(timezone.utc)

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            async for group in fetch_groups_via_api(network, client, settings.request_delay_seconds):
                seed = GroupSeed(
                    group_urlname=group["urlname"],
                    group_url=group.get("link") or f"https://www.meetup.com/{group['urlname']}/",
                    pro_network=network,
                    seeded_at=now,
                )
                publish(
                    producer,
                    topic=settings.topic_groups_to_scrape,
                    value=seed.model_dump(mode="json"),
                    key=seed.group_urlname,  # partition by group so msgs are ordered
                )
                published += 1
                log.info("[%s] Seeded: %s (%d so far)", network, seed.group_urlname, published)

            log.info("[%s] API seed complete: %d groups", network, published)
            return published

        except Exception as exc:
            log.warning("[%s] API failed (%s), trying Playwright fallback", network, exc)

            if not settings.playwright_fallback or browser is None:
                log.error("[%s] Playwright fallback disabled or unavailable. Giving up.", network)
                raise

    # Playwright fallback (outside httpx context — no need for it)
    async for group in fetch_groups_via_playwright(network, browser, settings.request_delay_seconds):
        seed = GroupSeed(
            group_urlname=group["urlname"],
            group_url=group["link"],
            pro_network=network,
            seeded_at=now,
        )
        publish(
            producer,
            topic=settings.topic_groups_to_scrape,
            value=seed.model_dump(mode="json"),
            key=seed.group_urlname,
        )
        published += 1
        log.info("[%s] Seeded (playwright): %s (%d so far)", network, seed.group_urlname, published)

    log.info("[%s] Playwright seed complete: %d groups", network, published)
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
        num_partitions=6,
    )

    producer = make_producer(settings)

    # Only start Playwright if fallback is enabled
    playwright_ctx = async_playwright() if settings.playwright_fallback else None
    playwright = await playwright_ctx.start() if playwright_ctx else None
    browser = await playwright.chromium.launch(headless=True) if playwright else None

    try:
        total = 0
        for network in settings.pro_networks:
            count = await seed_network(network, settings, producer, browser)
            total += count

        log.info("Flushing producer (waiting for all deliveries)…")
        producer.flush(timeout=30)
        log.info("Seed complete. Total groups published: %d", total)

    finally:
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()


def main() -> None:
    settings = Settings.from_env()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
