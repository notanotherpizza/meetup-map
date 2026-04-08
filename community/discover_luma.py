#!/usr/bin/env python3
"""
discover_luma.py — Discover Luma calendars via city pages and category pages.

Luma has no public discovery API. Instead it has:
  - lu.ma/<city-slug>       e.g. lu.ma/london, lu.ma/sf, lu.ma/nyc
  - lu.ma/<category-slug>   e.g. lu.ma/tech, lu.ma/ai, lu.ma/startup

Both render via Next.js and embed __NEXT_DATA__ JSON in the HTML.
Each event in the list has a `calendar` field containing the calendar slug,
which maps to lu.ma/<calendar-slug>.

Strategy:
  1. Fetch city + category pages
  2. Parse __NEXT_DATA__ to extract event calendar slugs
  3. Deduplicate against community/groups.txt
  4. Write new lu.ma/<slug> URLs to community/discovered_luma.txt

No auth required. No Playwright. Pure httpx.

Usage:
    python discover_luma.py
    python discover_luma.py --dry-run
    python discover_luma.py --output community/discovered_luma.txt

Requires:
    pip install httpx
"""

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://lu.ma"

# City slugs — these are Luma's own city pages, not exhaustive but good coverage
CITY_SLUGS = [
    # UK
    "london",
    "manchester",
    "edinburgh",
    "bristol",
    "dublin",
    # Europe
    "amsterdam",
    "berlin",
    "munich",
    "paris",
    "barcelona",
    "madrid",
    "lisbon",
    "stockholm",
    "copenhagen",
    "oslo",
    "helsinki",
    "zurich",
    "vienna",
    "warsaw",
    "prague",
    "brussels",
    "milan",
    # North America
    "sf",
    "nyc",
    "seattle",
    "austin",
    "boston",
    "chicago",
    "la",
    "denver",
    "toronto",
    "vancouver",
    # APAC
    "singapore",
    "sydney",
    "melbourne",
    "tokyo",
    "bangalore",
    "mumbai",
    "seoul",
    "hong-kong",
    "taipei",
    # LATAM / MEA
    "sao-paulo",
    "buenos-aires",
    "lagos",
    "nairobi",
    "cape-town",
    "tel-aviv",
    "dubai",
]

# Category slugs — confirmed working Luma topic pages.
# Removed 404s from test run: startup, education, developer, product, health, data, web3
CATEGORY_SLUGS = [
    "tech",
    "ai",
    "science",
    "design",
    "climate",
    "genai-sf",    # active AI/tech community aggregator pages
    "genai-nyc",
    "genai-london",
]

REQUEST_DELAY_S = 0.5  # polite crawling


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)


def extract_next_data(html: str) -> dict:
    match = NEXT_DATA_RE.search(html)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def extract_calendar_slugs(next_data: dict) -> set[str]:
    """
    Walk the __NEXT_DATA__ structure to find calendar slugs.
    Luma embeds event lists with calendar info. The structure varies slightly
    between city and category pages, so we do a recursive key search.
    """
    slugs: set[str] = set()
    _walk(next_data, slugs)
    return slugs


def _walk(obj: object, slugs: set[str]) -> None:
    if isinstance(obj, dict):
        # Calendar slug can appear as:
        #   {"calendar": {"api_id": "...", "slug": "my-calendar", ...}}
        #   {"url": "my-calendar", "type": "calendar"}
        #   {"calendar_api_id": "...", "calendar_slug": "my-calendar"}
        if "slug" in obj and isinstance(obj.get("slug"), str):
            # Only collect if this looks like a calendar node
            if obj.get("type") in ("calendar", "community") or "calendar" in str(obj.keys()):
                slugs.add(obj["slug"])
        # Also look for nested calendar objects
        if "calendar" in obj and isinstance(obj["calendar"], dict):
            cal = obj["calendar"]
            slug = cal.get("slug") or cal.get("url")
            if slug and isinstance(slug, str):
                slugs.add(slug)
        for v in obj.values():
            _walk(v, slugs)
    elif isinstance(obj, list):
        for item in obj:
            _walk(item, slugs)


def luma_url(slug: str) -> str:
    return f"https://lu.ma/{slug}"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

async def fetch_page(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        resp = await client.get(url, timeout=20, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        print(f"  {resp.status_code} — {url}")
        return None
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None


async def discover_from_page(client: httpx.AsyncClient, page_url: str) -> set[str]:
    html = await fetch_page(client, page_url)
    if not html:
        return set()
    next_data = extract_next_data(html)
    if not next_data:
        # Fallback: try scraping calendar links from raw HTML
        # Luma calendar URLs appear as href="/c/..." or href="/cal/..."
        # and as event host calendar links
        slugs = set()
        for match in re.finditer(r'href="/([\w-]+)"[^>]*data-type="calendar"', html):
            slugs.add(match.group(1))
        return slugs
    return extract_calendar_slugs(next_data)


async def discover(dry_run: bool, output_path: Path) -> None:
    existing: set[str] = set()

    groups_file = Path("community/groups.txt")
    if groups_file.exists():
        for line in groups_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                existing.add(line)

    if output_path.exists():
        for line in output_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                existing.add(line)

    existing_slugs = set()
    for url in existing:
        # Extract slug from https://lu.ma/<slug>
        if "lu.ma/" in url:
            slug = url.rstrip("/").split("lu.ma/")[-1]
            existing_slugs.add(slug)

    discovered_slugs: set[str] = set()

    pages = (
        [(f"city/{slug}", f"{BASE_URL}/{slug}") for slug in CITY_SLUGS]
        + [(f"category/{slug}", f"{BASE_URL}/{slug}") for slug in CATEGORY_SLUGS]
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; meetup-map-discovery/1.0; +https://github.com/notanotherpizza/meetup-map)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async with httpx.AsyncClient(headers=headers) as client:
        for i, (label, url) in enumerate(pages, 1):
            print(f"[{i}/{len(pages)}] {label}...", end=" ", flush=True)
            slugs = await discover_from_page(client, url)
            new = slugs - existing_slugs - discovered_slugs
            discovered_slugs.update(new)
            print(f"{len(slugs)} calendars found, {len(new)} new")
            await asyncio.sleep(REQUEST_DELAY_S)

    new_urls = sorted(
        {luma_url(slug) for slug in discovered_slugs}
        - existing
    )

    # Filter out obviously non-calendar slugs (event IDs, city pages themselves, etc.)
    # Luma event IDs are typically "evt-XXXX" patterns; city names we seeded aren't calendars
    seeded_slugs = set(CITY_SLUGS) | set(CATEGORY_SLUGS)
    new_urls = [
        url for url in new_urls
        if not any(url.endswith(f"/{slug}") or url.endswith(f"/{slug}/") for slug in seeded_slugs)
        and not re.search(r'/evt-[a-zA-Z0-9]+', url)
    ]

    print(f"\nDiscovered {len(new_urls)} new Luma calendars.")

    if dry_run:
        print("\n--- DRY RUN: would write ---")
        for url in new_urls[:20]:
            print(url)
        if len(new_urls) > 20:
            print(f"... and {len(new_urls) - 20} more")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a") as f:
        f.write(f"\n# Discovered by discover_luma.py — {len(new_urls)} calendars\n")
        for url in new_urls:
            f.write(url + "\n")

    print(f"Written to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover Luma calendars via city/category pages")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing")
    parser.add_argument(
        "--output",
        default="community/discovered_luma.txt",
        help="Output file (appended to). Default: community/discovered_luma.txt",
    )
    args = parser.parse_args()

    asyncio.run(discover(args.dry_run, Path(args.output)))


if __name__ == "__main__":
    main()