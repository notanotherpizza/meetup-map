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

from community.discovery_config import get_meetup_cities, get_meetup_keywords

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

KEYWORDS = get_meetup_keywords()
CITIES = get_meetup_cities()

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


async def discover(dry_run: bool, output_path: Path, keywords: list[str] | None = None) -> None:
    keywords = KEYWORDS if keywords is None else keywords

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
    total_combos = len(keywords) * len(CITIES)
    done = 0

    async with httpx.AsyncClient() as client:
        for keyword in keywords:
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
    parser.add_argument(
        "--keyword-shard",
        default=None,
        help="Run only this shard of the keyword list, as 'i/n' (0-indexed), e.g. '0/8'. "
             "Splits the keyword x city grid across n parallel runs so each finishes well "
             "within a CI job's time limit — the full grid (114 keywords x 367 cities, "
             "~42k combos at 0.5s/combo) needs ~5.8h minimum, which alone exceeds GitHub's "
             "6h hard job ceiling once real request latency is added.",
    )
    args = parser.parse_args()

    keywords = None
    if args.keyword_shard:
        i, n = (int(x) for x in args.keyword_shard.split("/"))
        keywords = KEYWORDS[i::n]

    asyncio.run(discover(args.dry_run, Path(args.output), keywords=keywords))


if __name__ == "__main__":
    main()