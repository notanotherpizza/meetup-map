#!/usr/bin/env python3
"""
discover_luma.py — Discover Luma calendars via city and category pages.

Strategy:
  Fetch each city/category page and extract calendar slugs from __NEXT_DATA__.
  Luma exposes no public pagination API — each page yields ~15-20 calendars,
  so coverage comes from breadth of seed pages rather than depth.

  Deduplicates against community/groups.txt and previously discovered file.
  Writes new lu.ma/<slug> URLs to community/discovered_luma.txt.

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
import re
import json
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://lu.ma"

CITY_SLUGS = [
    # UK
    "london", "manchester", "edinburgh", "bristol", "dublin",
    # Europe
    "amsterdam", "berlin", "munich", "paris", "barcelona", "madrid",
    "lisbon", "stockholm", "copenhagen", "oslo", "helsinki", "zurich",
    "vienna", "warsaw", "prague", "brussels", "milan",
    # North America
    "sf", "nyc", "seattle", "austin", "boston", "chicago", "la",
    "denver", "toronto", "vancouver",
    # APAC
    "singapore", "sydney", "melbourne", "tokyo", "bangalore", "mumbai",
    "seoul", "hong-kong", "taipei",
    # LATAM / MEA
    "sao-paulo", "buenos-aires", "lagos", "nairobi", "cape-town",
    "tel-aviv", "dubai",
]

# Confirmed 200 after redirect (checked April 2026).
# Removed 404s: health, education
CATEGORY_SLUGS = [
    "tech", "ai", "science", "design", "climate",
    "music", "sports", "finance", "crypto", "gaming", "wellness",
    "food", "travel",
    "genai-sf", "genai-nyc", "genai-london",
]

REQUEST_DELAY_S = 0.5

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)

# Slugs that are seed pages themselves, not calendars
_SEED_SLUGS = set(CITY_SLUGS) | set(CATEGORY_SLUGS)

# Luma event IDs look like "evt-XXXX"
_EVENT_ID_RE = re.compile(r'^evt-[a-zA-Z0-9]+$')


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def extract_next_data(html: str) -> dict:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def _walk(obj: object, slugs: set[str]) -> None:
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
            _walk(v, slugs)
    elif isinstance(obj, list):
        for item in obj:
            _walk(item, slugs)


def extract_calendar_slugs(next_data: dict) -> set[str]:
    slugs: set[str] = set()
    _walk(next_data, slugs)
    return slugs


def _is_valid_calendar_slug(slug: str) -> bool:
    if not slug or len(slug) < 2:
        return False
    if slug in _SEED_SLUGS:
        return False
    if _EVENT_ID_RE.match(slug):
        return False
    return True


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

async def fetch_html(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        resp = await client.get(url, timeout=20, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        print(f"  {resp.status_code} — {url}")
        return None
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def luma_url(slug: str) -> str:
    return f"https://lu.ma/{slug}"


async def discover(dry_run: bool, output_path: Path) -> None:
    existing: set[str] = set()
    for path in [Path("community/groups.txt"), output_path]:
        if path.exists():
            for line in path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    existing.add(line)

    existing_slugs: set[str] = set()
    for url in existing:
        if "lu.ma/" in url:
            slug = url.rstrip("/").split("lu.ma/")[-1]
            existing_slugs.add(slug)

    discovered_slugs: set[str] = set()

    all_tags = CITY_SLUGS + CATEGORY_SLUGS
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; meetup-map-discovery/1.0)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async with httpx.AsyncClient(headers=headers) as client:
        for i, tag in enumerate(all_tags, 1):
            label = "city" if tag in CITY_SLUGS else "category"
            print(f"[{i}/{len(all_tags)}] {label}/{tag}...", end=" ", flush=True)

            html = await fetch_html(client, f"{BASE_URL}/{tag}")
            slugs: set[str] = set()
            if html:
                next_data = extract_next_data(html)
                if next_data:
                    raw = extract_calendar_slugs(next_data)
                    slugs = {s for s in raw if _is_valid_calendar_slug(s)}

            new = slugs - existing_slugs - discovered_slugs
            discovered_slugs.update(new)
            print(f"{len(slugs)} calendars, {len(new)} new")
            await asyncio.sleep(REQUEST_DELAY_S)

    new_urls = sorted(
        luma_url(s) for s in discovered_slugs
        if luma_url(s) not in existing
    )

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
    parser = argparse.ArgumentParser(
        description="Discover Luma calendars via city/category pages (__NEXT_DATA__ scrape)"
    )
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