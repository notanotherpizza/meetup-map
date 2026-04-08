#!/usr/bin/env python3
"""
discover_meetup.py — Discover Meetup groups via keyword × location grid.
No API token required. Uses meetup.com/find/ public search pages + __NEXT_DATA__.

Uses source=EVENTS (not source=GROUPS — that returns a JS-hydrated shell with
no data in __NEXT_DATA__). Event nodes each embed their host group's urlname,
so we get groups via events.

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
import re
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.meetup.com/find/"

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

# Meetup location format: "<country_code>--<City_Name>" (spaces as underscores)
# Confirmed from live Meetup search URLs e.g. ?location=gb--London, ?location=pt--Lagos
CITIES = [
    # UK & Ireland
    ("gb", "London"),
    ("gb", "Manchester"),
    ("gb", "Edinburgh"),
    ("gb", "Bristol"),
    ("gb", "Birmingham"),
    ("gb", "Leeds"),
    ("ie", "Dublin"),
    # Western Europe
    ("nl", "Amsterdam"),
    ("de", "Berlin"),
    ("de", "Hamburg"),
    ("de", "Munich"),
    ("fr", "Paris"),
    ("es", "Barcelona"),
    ("es", "Madrid"),
    ("pt", "Lisbon"),
    ("se", "Stockholm"),
    ("dk", "Copenhagen"),
    ("no", "Oslo"),
    ("fi", "Helsinki"),
    ("ch", "Zurich"),
    ("at", "Vienna"),
    ("pl", "Warsaw"),
    ("cz", "Prague"),
    ("be", "Brussels"),
    ("it", "Milan"),
    ("it", "Rome"),
    # North America
    ("us", "San_Francisco"),
    ("us", "New_York"),
    ("us", "Seattle"),
    ("us", "Austin"),
    ("us", "Boston"),
    ("us", "Chicago"),
    ("us", "Los_Angeles"),
    ("us", "Denver"),
    ("ca", "Toronto"),
    ("ca", "Vancouver"),
    ("ca", "Montreal"),
    # APAC
    ("sg", "Singapore"),
    ("au", "Sydney"),
    ("au", "Melbourne"),
    ("jp", "Tokyo"),
    ("in", "Bangalore"),
    ("in", "Mumbai"),
    ("kr", "Seoul"),
    ("hk", "Hong_Kong"),
    ("tw", "Taipei"),
    ("id", "Jakarta"),
    # LATAM / MEA
    ("br", "Sao_Paulo"),
    ("ar", "Buenos_Aires"),
    ("co", "Bogota"),
    ("ng", "Lagos"),
    ("ke", "Nairobi"),
    ("za", "Cape_Town"),
    ("il", "Tel_Aviv"),
    ("ae", "Dubai"),
]

REQUEST_DELAY_S = 0.5

NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)

_NON_GROUP_PATHS = {
    "find", "topics", "home", "search", "about", "help", "messages",
    "meetup_api", "privacy", "terms", "jobs", "pro", "lp", "apps",
    "signin", "register", "login", "dashboard", "account", "blog",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_url(keyword: str, country: str, city: str) -> str:
    loc = f"{country}--{city}"
    kw_encoded = keyword.replace(" ", "+")
    # source=EVENTS embeds group urlnames in __NEXT_DATA__ event nodes.
    # source=GROUPS is JS-hydrated client-side — __NEXT_DATA__ is an empty shell.
    return f"{BASE_URL}?keywords={kw_encoded}&location={loc}&source=EVENTS"


def extract_urlnames(next_data: dict) -> set[str]:
    """
    Recursively walk __NEXT_DATA__ to collect group urlnames.
    Event nodes embed their host group as {"__typename": "Group", "urlname": "..."}
    """
    urlnames: set[str] = set()
    _walk(next_data, urlnames)
    return urlnames


def _walk(obj: object, urlnames: set[str]) -> None:
    if isinstance(obj, dict):
        # Primary: typed Group nodes
        if obj.get("__typename") == "Group" and "urlname" in obj:
            urlnames.add(obj["urlname"])
        # Secondary: any dict with a urlname that looks like a slug
        elif "urlname" in obj and isinstance(obj["urlname"], str):
            slug = obj["urlname"]
            if slug and "/" not in slug and "." not in slug and slug not in _NON_GROUP_PATHS:
                urlnames.add(slug)
        for v in obj.values():
            _walk(v, urlnames)
    elif isinstance(obj, list):
        for item in obj:
            _walk(item, urlnames)


def extract_next_data(html: str) -> dict:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def meetup_url(urlname: str) -> str:
    return f"https://www.meetup.com/{urlname}/"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

async def fetch_page(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        resp = await client.get(url, timeout=20, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 429:
            print("  429 rate limited — sleeping 15s")
            await asyncio.sleep(15)
            return None
        else:
            print(f"  {resp.status_code} — {url}")
            return None
    except httpx.TimeoutException:
        print(f"  TIMEOUT — {url}")
        return None
    except Exception as e:
        print(f"  ERROR — {url}: {e}")
        return None


async def discover_from_page(client: httpx.AsyncClient, url: str) -> set[str]:
    html = await fetch_page(client, url)
    if not html:
        return set()
    next_data = extract_next_data(html)
    if next_data:
        return extract_urlnames(next_data)
    # Fallback: regex scan for group urlnames in raw HTML
    slugs = set()
    for m in re.finditer(r'"urlname":"([\w-]+)"', html):
        slug = m.group(1)
        if slug not in _NON_GROUP_PATHS and len(slug) > 3:
            slugs.add(slug)
    return slugs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
                existing_slugs.add(slug)

    discovered_slugs: set[str] = set()
    total = len(KEYWORDS) * len(CITIES)
    done = 0

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    async with httpx.AsyncClient(headers=headers) as client:
        for keyword in KEYWORDS:
            for country, city in CITIES:
                done += 1
                url = build_url(keyword, country, city)
                print(
                    f"[{done}/{total}] {keyword!r} near {city.replace('_', ' ')}...",
                    end=" ",
                    flush=True,
                )
                slugs = await discover_from_page(client, url)
                new = slugs - existing_slugs - discovered_slugs
                discovered_slugs.update(new)
                print(f"{len(slugs)} groups, {len(new)} new")
                await asyncio.sleep(REQUEST_DELAY_S)

    new_urls = sorted(
        {meetup_url(s) for s in discovered_slugs} - existing
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
        description="Discover Meetup groups via keyword x location grid (no auth required)"
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