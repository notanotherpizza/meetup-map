"""
worker/platforms/luma.py
────────────────────────
Luma (luma.com) platform scraper.

Implements the Platform interface using plain httpx — no Playwright needed.
Luma embeds full event data in __NEXT_DATA__ on every page, accessible
without JavaScript execution.

Scraping strategy:
  1. GET luma.com/<slug> — parse __NEXT_DATA__ for calendar metadata +
     upcoming event slugs (featured_items), then GET the same URL with
     ?period=past to get past event slugs from page links
  2. GET luma.com/<event_slug> for each event — parse __NEXT_DATA__ for
     full event data

Venue geocoding: Luma provides Google place_id, full address, and coordinates
on event pages via the `coordinate` field. We pass geocode_source as
'luma_google' to indicate pre-geocoded data — no Nominatim needed.
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

LUMA_BASE = "https://luma.com"
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

        # Normalise URL: lu.ma → luma.com
        url = seed.group_url.replace("lu.ma/", "luma.com/")
        slug = url.rstrip("/").split("/")[-1]

        # ── Step 1: calendar page ─────────────────────────────────────────
        calendar_data, upcoming_items, past_slugs = await self._scrape_calendar(
            slug, http_client
        )

        log.info("  Luma %s: %d upcoming items, %d past slugs",
                 seed.group_urlname, len(upcoming_items), len(past_slugs))

        if max_past_events > 0:
            past_slugs = past_slugs[:max_past_events]

        # ── Step 2: events ────────────────────────────────────────────────
        past_events:     list[EventRaw] = []
        upcoming_events: list[EventRaw] = []
        venues:          list[VenueRaw] = []
        seen_venues:     set[str] = set()
        events_scrape_ok = False

        try:
            # Upcoming: built directly from featured_items — no extra HTTP requests
            for item in upcoming_items:
                event_raw, venue_raw = _event_from_featured_item(
                    item, seed.group_urlname, now
                )
                if event_raw:
                    upcoming_events.append(event_raw)
                if venue_raw and venue_raw.venue_id not in seen_venues:
                    venues.append(venue_raw)
                    seen_venues.add(venue_raw.venue_id)

            # Past: still requires per-event page fetch
            for ev_slug in past_slugs:
                try:
                    event_raw, venue_raw = await self._scrape_event(
                        ev_slug, seed.group_urlname, "past", now, http_client
                    )
                    if event_raw:
                        past_events.append(event_raw)
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
    ) -> tuple[dict, list[dict], list[str]]:
        """Returns (calendar_dict, upcoming_featured_items, past_slugs).

        upcoming_featured_items are the raw item dicts from featured_items —
        they already contain full event + coordinate data so we don't need
        to fetch individual event pages for upcoming events.
        """
        url = f"{LUMA_BASE}/{slug}"
        data = await self._fetch_next_data(url, client)
        props = (data.get("props", {})
                     .get("pageProps", {})
                     .get("initialData", {})
                     .get("data", {}))

        if not props:
            raise ValueError(f"No __NEXT_DATA__ pageProps at {url}")

        calendar = props.get("calendar", {})
        upcoming_items = props.get("featured_items", [])

        # Past: fetch ?period=past and extract event slugs from href links.
        # Luma now uses /event/<slug> paths on luma.com.
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
            # Match both legacy bare slugs (/rwwjsa5z) and new /event/<slug> paths
            seen: set[str] = set()
            SKIP = {"discover", "signin", "home", slug, "event", "calendar"}
            for href in re.findall(r'href="/([^"]+)"', resp.text):
                # Strip /event/ prefix if present
                parts = href.strip("/").split("/")
                ev_slug = parts[-1]
                if (
                    ev_slug
                    and ev_slug not in seen
                    and ev_slug not in SKIP
                    and not ev_slug.startswith("cal-")
                    and not ev_slug.startswith("usr-")
                    and len(ev_slug) >= 6
                ):
                    seen.add(ev_slug)
                    past_slugs.append(ev_slug)
        except Exception as exc:
            log.warning("  Could not fetch past slugs for %s: %s", slug, exc)

        return calendar, upcoming_items, past_slugs

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

        guest_count = props.get("guest_count")
        rsvp_count  = int(guest_count) if guest_count is not None else None

        return _build_event_and_venue(
            event=event,
            event_id=event_id,
            event_url=url,
            group_urlname=group_urlname,
            status=status,
            rsvp_count=rsvp_count,
            now=now,
            scrape_method="httpx_nextdata",
        )

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


# ── Module-level helpers ──────────────────────────────────────────────────────

def _event_from_featured_item(
    item: dict,
    group_urlname: str,
    now: datetime,
) -> tuple[Optional[EventRaw], Optional[VenueRaw]]:
    """Build EventRaw + VenueRaw from a featured_items entry.

    The calendar page embeds full event data including coordinates in
    featured_items, so upcoming events don't need a separate page fetch.
    """
    event = item.get("event") or {}
    event_id = event.get("api_id")
    if not event_id:
        return None, None

    ev_slug = event.get("url")
    event_url = f"{LUMA_BASE}/{ev_slug}" if ev_slug else None

    guest_count = item.get("guest_count") or item.get("ticket_count")
    rsvp_count  = int(guest_count) if guest_count is not None else None

    return _build_event_and_venue(
        event=event,
        event_id=event_id,
        event_url=event_url,
        group_urlname=group_urlname,
        status="upcoming",
        rsvp_count=rsvp_count,
        now=now,
        scrape_method="httpx_nextdata_calendar",
    )


def _build_event_and_venue(
    event: dict,
    event_id: str,
    event_url: Optional[str],
    group_urlname: str,
    status: str,
    rsvp_count: Optional[int],
    now: datetime,
    scrape_method: str,
) -> tuple[Optional[EventRaw], Optional[VenueRaw]]:
    """Shared builder for EventRaw + VenueRaw from a Luma event dict.

    Coordinates come from event.coordinate (Google-geocoded), not
    geo_address_info which only carries address strings.
    """
    is_online = event.get("location_type") == "online"
    starts_at = _parse_dt(event.get("start_at"))
    ends_at   = _parse_dt(event.get("end_at"))

    venue_raw: Optional[VenueRaw] = None
    venue_id:  Optional[str] = None

    if not is_online:
        geo      = event.get("geo_address_info") or {}
        place_id = geo.get("place_id")
        coord    = event.get("coordinate") or {}

        if place_id:
            venue_id = f"luma_{place_id}"
            localized   = geo.get("localized") or {}
            locale_data = (
                localized.get("en-GB") or
                next(iter(localized.values()), {})
            )
            full_address = (
                locale_data.get("full_address") or geo.get("full_address") or geo.get("address")
            )
            venue_raw = VenueRaw(
                venue_id=venue_id,
                name=geo.get("address"),
                address=full_address,
                city=geo.get("city"),
                state=geo.get("region"),
                country=geo.get("country"),
                # Coordinates live on event.coordinate, not geo_address_info
                lat=_safe_float(coord.get("latitude")),
                lon=_safe_float(coord.get("longitude")),
                geocode_source="luma_google",
                scraped_at=now,
            )

    event_raw = EventRaw(
        event_id=event_id,
        group_urlname=group_urlname,
        title=event.get("name", ""),
        event_url=event_url,
        status=status,
        is_online=is_online,
        venue_id=venue_id,
        starts_at=starts_at,
        ends_at=ends_at,
        rsvp_count=rsvp_count,
        scraped_at=now,
        scrape_method=scrape_method,
    )

    return event_raw, venue_raw


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