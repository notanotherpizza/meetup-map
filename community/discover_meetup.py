#!/usr/bin/env python3
"""
discover_meetup.py — Discover Meetup groups via keyword × location grid.
No API token required. Uses Meetup's GQL2 endpoint directly.

Posts eventSearch queries with lat/lon/radius/keyword, paginating via
endCursor until hasNextPage is False. Each event node embeds its host
group's urlname, so we collect unique groups across all pages.

Outputs one group URL per line, deduplicating against community/groups.txt
and any previously discovered file.

Usage:
    python discover_meetup.py
    python discover_meetup.py --dry-run
    python discover_meetup.py --output community/discovered_meetup.txt

Requires:
    pip install httpx
"""

import argparse
import asyncio
import json
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GQL_URL = "https://www.meetup.com/gql2"

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

KEYWORDS = [
    "python",
    "data science",
    "machine learning",
    "artificial intelligence",
    "AI",
    "MLOps",
    "LLM",
    "kafka",
    "data engineering",
    "analytics",
    "rust",
    "golang",
    "kubernetes",
    "devops",
    "cloud native",
    "open source",
    "javascript",
    "react",
    "typescript",
    "backend",
    "platform engineering",
    "developer",
    "tech meetup",
    "startup",
    "product",
    "UX",
    "security",
    "data",
]

# (label, lat, lon, radius_miles)
# Radius 50 miles gives good city coverage without too much overlap.
# Larger cities like London/NYC get 30 miles; broader regions get 75.
CITIES = [
    # UK & Ireland
    ("London",          51.51,  -0.12,  30),
    ("Manchester",      53.48,  -2.24,  30),
    ("Edinburgh",       55.95,  -3.19,  30),
    ("Bristol",         51.45,  -2.59,  30),
    ("Birmingham",      52.48,  -1.90,  30),
    ("Leeds",           53.80,  -1.55,  30),
    ("Dublin",          53.33,  -6.25,  30),
    # Western Europe
    ("Amsterdam",       52.37,   4.90,  30),
    ("Berlin",          52.52,  13.40,  30),
    ("Hamburg",         53.55,   9.99,  30),
    ("Munich",          48.14,  11.58,  30),
    ("Paris",           48.86,   2.35,  30),
    ("Barcelona",       41.39,   2.16,  30),
    ("Madrid",          40.42,  -3.70,  30),
    ("Lisbon",          38.72,  -9.14,  30),
    ("Stockholm",       59.33,  18.07,  30),
    ("Copenhagen",      55.68,  12.57,  30),
    ("Oslo",            59.91,  10.75,  30),
    ("Helsinki",        60.17,  24.94,  30),
    ("Zurich",          47.38,   8.54,  30),
    ("Vienna",          48.21,  16.37,  30),
    ("Warsaw",          52.23,  21.01,  30),
    ("Prague",          50.08,  14.44,  30),
    ("Brussels",        50.85,   4.35,  30),
    ("Milan",           45.46,   9.19,  30),
    ("Rome",            41.90,  12.50,  30),
    # North America
    ("San Francisco",   37.77, -122.42, 30),
    ("New York",        40.71,  -74.01, 30),
    ("Seattle",         47.61, -122.33, 30),
    ("Austin",          30.27,  -97.74, 30),
    ("Boston",          42.36,  -71.06, 30),
    ("Chicago",         41.88,  -87.63, 30),
    ("Los Angeles",     34.05, -118.24, 30),
    ("Denver",          39.74, -104.98, 30),
    ("Toronto",         43.65,  -79.38, 30),
    ("Vancouver",       49.25, -123.12, 30),
    ("Montreal",        45.51,  -73.55, 30),
    # APAC
    ("Singapore",        1.35,  103.82, 30),
    ("Sydney",         -33.87,  151.21, 30),
    ("Melbourne",      -37.81,  144.96, 30),
    ("Tokyo",           35.69,  139.69, 30),
    ("Bangalore",       12.97,   77.59, 30),
    ("Mumbai",          19.08,   72.88, 30),
    ("Seoul",           37.57,  126.98, 30),
    ("Hong Kong",       22.32,  114.17, 30),
    ("Taipei",          25.05,  121.53, 30),
    ("Jakarta",         -6.21,  106.85, 30),
    # LATAM / MEA
    ("Sao Paulo",       -23.55, -46.63, 30),
    ("Buenos Aires",    -34.60, -58.38, 30),
    ("Bogota",           4.71,  -74.07, 30),
    ("Lagos",            6.52,    3.38, 30),
    ("Nairobi",         -1.29,   36.82, 30),
    ("Cape Town",       -33.93,  18.42, 30),
    ("Tel Aviv",        32.08,   34.78, 30),
    ("Dubai",           25.20,   55.27, 30),
]

GQL_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetup-map-discovery/1.0)",
}

PAGE_SIZE = 20
MAX_PAGES = 25        # 25 × 20 = 500 events max per keyword×city
REQUEST_DELAY_S = 0.5
RATE_LIMIT_SLEEP_S = 30


# ---------------------------------------------------------------------------
# GQL fetch + pagination
# ---------------------------------------------------------------------------

async def fetch_event_search_page(
    client: httpx.AsyncClient,
    keyword: str,
    lat: float,
    lon: float,
    radius: int,
    cursor: str | None,
) -> dict | None:
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
        resp = await client.post(GQL_URL, json=payload, headers=GQL_HEADERS, timeout=20)
        if resp.status_code == 429:
            print(f"  429 rate limited — sleeping {RATE_LIMIT_SLEEP_S}s")
            await asyncio.sleep(RATE_LIMIT_SLEEP_S)
            return None
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            print(f"  GQL errors: {data['errors']}")
            return None
        return data.get("data", {}).get("eventSearch")
    except httpx.TimeoutException:
        print(f"  TIMEOUT — {keyword!r} @ ({lat},{lon})")
        return None
    except Exception as e:
        print(f"  ERROR — {keyword!r} @ ({lat},{lon}): {e}")
        return None


async def discover_groups_for_combo(
    client: httpx.AsyncClient,
    keyword: str,
    label: str,
    lat: float,
    lon: float,
    radius: int,
) -> set[str]:
    """Paginate through all eventSearch results for one keyword × city combo."""
    all_urlnames: set[str] = set()
    cursor: str | None = None
    pages = 0

    while pages < MAX_PAGES:
        result = await fetch_event_search_page(client, keyword, lat, lon, radius, cursor)
        pages += 1

        if not result:
            break

        for edge in result.get("edges", []):
            group = edge.get("node", {}).get("group") or {}
            urlname = group.get("urlname")
            if urlname:
                all_urlnames.add(urlname)

        page_info = result.get("pageInfo", {})
        if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
            break

        cursor = page_info["endCursor"]
        await asyncio.sleep(REQUEST_DELAY_S)

    return all_urlnames


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def meetup_url(urlname: str) -> str:
    return f"https://www.meetup.com/{urlname}/"


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
        if "meetup.com/" in url:
            slug = url.rstrip("/").split("meetup.com/")[-1].split("/")[0]
            if slug:
                existing_slugs.add(slug.lower())

    discovered_slugs: set[str] = set()
    total_combos = len(KEYWORDS) * len(CITIES)
    done = 0

    async with httpx.AsyncClient() as client:
        for keyword in KEYWORDS:
            for label, lat, lon, radius in CITIES:
                done += 1
                print(
                    f"[{done}/{total_combos}] {keyword!r} near {label}...",
                    end=" ", flush=True,
                )
                slugs = await discover_groups_for_combo(
                    client, keyword, label, lat, lon, radius
                )
                new = {s for s in slugs if s.lower() not in existing_slugs
                       and s.lower() not in {d.lower() for d in discovered_slugs}}
                discovered_slugs.update(new)
                print(f"{len(slugs)} groups, {len(new)} new")
                await asyncio.sleep(REQUEST_DELAY_S)

    new_urls = sorted(
        meetup_url(s) for s in discovered_slugs
        if meetup_url(s) not in existing
    )

    print(f"\nDiscovered {len(new_urls)} new Meetup groups.")

    if dry_run:
        print("\n--- DRY RUN: would write ---")
        for url in new_urls[:20]:
            print(url)
        if len(new_urls) > 20:
            print(f"... and {len(new_urls) - 20} more")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a") as f:
        f.write(f"\n# Discovered by discover_meetup.py — {len(new_urls)} groups\n")
        for url in new_urls:
            f.write(url + "\n")

    print(f"Written to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover Meetup groups via keyword x location grid (GQL, no auth required)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing")
    parser.add_argument(
        "--output",
        default="community/discovered_meetup.txt",
        help="Output file (appended to). Default: community/discovered_meetup.txt",
    )
    args = parser.parse_args()
    asyncio.run(discover(args.dry_run, Path(args.output)))


if __name__ == "__main__":
    main()