"""
worker/platforms/luma.py
────────────────────────
Luma (lu.ma) platform scraper.

Implements the Platform interface using plain httpx — no Playwright needed.
Luma embeds full event data in __NEXT_DATA__ on every page, accessible
without JavaScript execution.

Scraping strategy:
  1. GET lu.ma/<slug> — parse __NEXT_DATA__ for calendar metadata +
     upcoming event slugs (featured_items), then GET the same URL with
     ?period=past to get past event slugs from page links
  2. GET lu.ma/<event_slug> for each event — parse __NEXT_DATA__ for
     full event data

Venue geocoding: Luma provides Google place_id and full address on event
pages, so lat/lon is not needed from Nominatim. We pass geocode_source
as 'luma_google' to indicate pre-geocoded data.
"""
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw
from worker.platforms.base import Platform, ScrapeResult

log = logging.getLogger(__name__)

LUMA_BASE = "https://lu.ma"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}
INTER_REQUEST_DELAY = 0.5  # seconds between requests
REQUEST_TIMEOUT = 15       # seconds


class LumaPlatform(Platform):

    def can_handle(self, url: str) -> bool:
        return "lu.ma" in url or "luma.com" in url

    async def scrape(
        self,
        seed: GroupSeed,
        browser,  # unused — kept for interface compatibility
        http_client: httpx.AsyncClient,
        max_past_events: int,
        worker_id: str,
    ) -> ScrapeResult:
        t_start = asyncio.get_event_loop().time()
        now = datetime.now(timezone.utc)

        # Normalise URL: luma.com → lu.ma
        url = seed.group_url.replace("luma.com/", "lu.ma/")
        slug = url.rstrip("/").split("/")[-1]

        # ── Step 1: calendar page ─────────────────────────────────────────
        calendar_data, upcoming_slugs, past_slugs = await self._scrape_calendar(
            slug, http_client
        )

        log.info("  Luma %s: %d upcoming slugs, %d past slugs",
                 seed.group_urlname, len(upcoming_slugs), len(past_slugs))

        if max_past_events > 0:
            past_slugs = past_slugs[:max_past_events]

        # ── Step 2: scrape individual event pages ─────────────────────────
        past_events:    list[EventRaw] = []
        upcoming_events: list[EventRaw] = []
        venues:         list[VenueRaw] = []
        seen_venues:    set[str] = set()
        events_scrape_ok = False

        all_slugs = (
            [("upcoming", s) for s in upcoming_slugs] +
            [("past",     s) for s in past_slugs]
        )

        try:
            for status, ev_slug in all_slugs:
                try:
                    event_raw, venue_raw = await self._scrape_event(
                        ev_slug, seed.group_urlname, status, now, http_client
                    )
                    if event_raw:
                        if status == "past":
                            past_events.append(event_raw)
                        else:
                            upcoming_events.append(event_raw)
                    if venue_raw and venue_raw.venue_id not in seen_venues:
                        venues.append(venue_raw)
                        seen_venues.add(venue_raw.venue_id)
                except Exception as exc:
                    log.warning("  Failed to scrape Luma event %s: %s", ev_slug, exc)
                await asyncio.sleep(INTER_REQUEST_DELAY)

            events_scrape_ok = True
        except Exception as exc:
            log.warning("  Luma events scrape failed for %s: %s",
                        seed.group_urlname, exc)

        duration_ms = int((asyncio.get_event_loop().time() - t_start) * 1000)

        group = GroupRaw(
            group_urlname=seed.group_urlname,
            name=calendar_data.get("name") or seed.group_urlname,
            pro_network=seed.pro_network,
            platform="luma",
            city=calendar_data.get("geo_city") or seed.city,
            country=calendar_data.get("geo_country") or seed.country,
            lat=_safe_float(
                (calendar_data.get("coordinate") or {}).get("latitude")
            ),
            lon=_safe_float(
                (calendar_data.get("coordinate") or {}).get("longitude")
            ),
            member_count=None,  # Luma doesn't expose subscriber count publicly
            source_url=seed.group_url,
            scraped_at=now,
            scrape_method="httpx_nextdata",
            events_scrape_ok=events_scrape_ok,
            total_past_events=len(past_slugs),
            worker_id=worker_id,
            scrape_duration_ms=duration_ms,
        )

        log.info("  -> %d past, %d upcoming events | events_ok: %s | duration: %dms",
                 len(past_events), len(upcoming_events), events_scrape_ok, duration_ms)

        return ScrapeResult(
            group=group,
            venues=venues,
            past_events=past_events,
            upcoming_events=upcoming_events,
        )

    # ── Calendar page ─────────────────────────────────────────────────────

    async def _scrape_calendar(
        self,
        slug: str,
        client: httpx.AsyncClient,
    ) -> tuple[dict, list[str], list[str]]:
        """Returns (calendar_dict, upcoming_slugs, past_slugs)."""
        url = f"{LUMA_BASE}/{slug}"
        data = await self._fetch_next_data(url, client)
        props = (data.get("props", {})
                     .get("pageProps", {})
                     .get("initialData", {})
                     .get("data", {}))

        if not props:
            raise ValueError(f"No __NEXT_DATA__ pageProps at {url}")

        calendar = props.get("calendar", {})

        # Upcoming: from featured_items
        upcoming_slugs: list[str] = []
        for item in props.get("featured_items", []):
            ev_slug = (item.get("event") or {}).get("url")
            if ev_slug:
                upcoming_slugs.append(ev_slug)

        # Past: fetch the ?period=past variant and scrape links from HTML
        past_slugs: list[str] = []
        await asyncio.sleep(INTER_REQUEST_DELAY)
        try:
            resp = await client.get(
                f"{url}?period=past",
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
            )
            resp.raise_for_status()
            # Extract all /slug links that look like event slugs
            links = re.findall(r'href="(/([a-z0-9]{8,}))"', resp.text)
            seen: set[str] = set()
            for _, ev_slug in links:
                if ev_slug not in seen and ev_slug not in (
                    "discover", "signin", "home", slug
                ) and not ev_slug.startswith("cal-"):
                    seen.add(ev_slug)
                    past_slugs.append(ev_slug)
        except Exception as exc:
            log.warning("  Could not fetch past slugs for %s: %s", slug, exc)

        return calendar, upcoming_slugs, past_slugs

    # ── Event page ────────────────────────────────────────────────────────

    async def _scrape_event(
        self,
        slug: str,
        group_urlname: str,
        status: str,
        now: datetime,
        client: httpx.AsyncClient,
    ) -> tuple[Optional[EventRaw], Optional[VenueRaw]]:
        url = f"{LUMA_BASE}/{slug}"
        data = await self._fetch_next_data(url, client)
        props = (data.get("props", {})
                     .get("pageProps", {})
                     .get("initialData", {})
                     .get("data", {}))

        if not props:
            log.warning("  No __NEXT_DATA__ for event %s", slug)
            return None, None

        event = props.get("event") or {}
        event_id = event.get("api_id") or props.get("api_id")
        if not event_id:
            return None, None

        title    = event.get("name", "")
        event_url = url
        is_online = event.get("location_type") == "online"
        starts_at = _parse_dt(event.get("start_at"))
        ends_at   = _parse_dt(event.get("end_at"))

        guest_count = props.get("guest_count")
        rsvp_count  = int(guest_count) if guest_count is not None else None

        # ── Venue ─────────────────────────────────────────────────────────
        venue_raw: Optional[VenueRaw] = None
        venue_id:  Optional[str] = None

        if not is_online:
            geo      = event.get("geo_address_info") or {}
            place_id = geo.get("place_id")
            if place_id:
                venue_id = f"luma_{place_id}"
                localized   = geo.get("localized") or {}
                locale_data = (
                    localized.get("en-GB") or
                    next(iter(localized.values()), {})
                )
                full_address = (
                    locale_data.get("full_address") or geo.get("address")
                )
                venue_raw = VenueRaw(
                    venue_id=venue_id,
                    name=geo.get("address"),
                    address=full_address,
                    city=geo.get("city"),
                    state=geo.get("region"),
                    country=geo.get("country"),
                    geocode_source="luma_google",
                    scraped_at=now,
                )

        event_raw = EventRaw(
            event_id=event_id,
            group_urlname=group_urlname,
            title=title,
            event_url=event_url,
            status=status,
            is_online=is_online,
            venue_id=venue_id,
            starts_at=starts_at,
            ends_at=ends_at,
            rsvp_count=rsvp_count,
            scraped_at=now,
            scrape_method="httpx_nextdata",
        )

        return event_raw, venue_raw

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _fetch_next_data(
        self,
        url: str,
        client: httpx.AsyncClient,
    ) -> dict:
        resp = await client.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
        )
        resp.raise_for_status()
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text,
            re.DOTALL,
        )
        if not m:
            return {}
        try:
            return json.loads(m.group(1))
        except Exception:
            return {}


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _parse_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None