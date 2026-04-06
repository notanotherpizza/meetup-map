"""
worker/platforms/luma.py
────────────────────────
Luma (lu.ma) platform scraper.

Implements the Platform interface using Playwright to scrape __NEXT_DATA__
from calendar and event pages. No API key required.

Scraping strategy:
  1. Load lu.ma/<slug> — extract calendar metadata + upcoming event slugs
     from featured_items, then click "Past" tab for past event slugs
  2. Load lu.ma/<event_slug> for each event — extract full event data
     from __NEXT_DATA__

Venue geocoding: Luma provides Google place_id and full address on event
pages, so lat/lon is not needed from Nominatim. We pass geocode_source
as 'luma_google' to indicate pre-geocoded data.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from playwright.async_api import Browser, BrowserContext

from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw
from worker.platforms.base import Platform, ScrapeResult

log = logging.getLogger(__name__)

LUMA_BASE = "https://lu.ma"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
PAGE_TIMEOUT = 30_000   # ms
INTER_PAGE_DELAY = 0.5  # seconds between event page loads


class LumaPlatform(Platform):

    def can_handle(self, url: str) -> bool:
        return "lu.ma" in url or "luma.com" in url

    async def scrape(
        self,
        seed: GroupSeed,
        browser: Browser,
        http_client: httpx.AsyncClient,
        max_past_events: int,
        worker_id: str,
    ) -> ScrapeResult:
        t_start = asyncio.get_event_loop().time()
        now = datetime.now(timezone.utc)

        context: BrowserContext = await browser.new_context(user_agent=USER_AGENT)
        try:
            # ── Step 1: calendar page ─────────────────────────────────────
            cal_page = await context.new_page()
            calendar_data, upcoming_slugs, past_slugs = await self._scrape_calendar(
                cal_page, seed.group_url
            )
            await cal_page.close()

            log.info("  Luma %s: %d upcoming slugs, %d past slugs",
                     seed.group_urlname, len(upcoming_slugs), len(past_slugs))

            # ── Step 2: scrape individual event pages ─────────────────────
            if max_past_events > 0:
                past_slugs = past_slugs[:max_past_events]

            past_events: list[EventRaw] = []
            upcoming_events: list[EventRaw] = []
            venues: list[VenueRaw] = []
            seen_venues: set[str] = set()
            events_scrape_ok = False

            all_slugs = [("upcoming", s) for s in upcoming_slugs] + \
                        [("past", s) for s in past_slugs]

            try:
                for status, slug in all_slugs:
                    ev_page = await context.new_page()
                    try:
                        event_raw, venue_raw = await self._scrape_event(
                            ev_page, slug, seed.group_urlname, status, now
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
                        log.warning("  Failed to scrape Luma event %s: %s", slug, exc)
                    finally:
                        await ev_page.close()
                    await asyncio.sleep(INTER_PAGE_DELAY)

                events_scrape_ok = True
            except Exception as exc:
                log.warning("  Luma events scrape failed for %s: %s", seed.group_urlname, exc)

        finally:
            await context.close()

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
            member_count=None,        # Luma doesn't expose subscriber count publicly
            source_url=seed.group_url,
            scraped_at=now,
            scrape_method="playwright_nextdata",
            events_scrape_ok=events_scrape_ok,
            total_past_events=len(past_slugs),
            worker_id=worker_id,
            scrape_duration_ms=duration_ms,
        )

        log.info("  -> %d past, %d upcoming events | events_ok: %s",
                 len(past_events), len(upcoming_events), events_scrape_ok)

        return ScrapeResult(
            group=group,
            venues=venues,
            past_events=past_events,
            upcoming_events=upcoming_events,
        )

    # ── Calendar page ─────────────────────────────────────────────────────

    async def _scrape_calendar(
        self,
        page,
        url: str,
    ) -> tuple[dict, list[str], list[str]]:
        """
        Returns (calendar_dict, upcoming_slugs, past_slugs).
        """
        await page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT)
        await page.wait_for_timeout(2000)

        next_data = await self._get_next_data(page)
        props = (next_data.get("props", {})
                          .get("pageProps", {})
                          .get("initialData", {})
                          .get("data", {}))

        if not props:
            raise ValueError(f"No __NEXT_DATA__ pageProps found at {url}")

        calendar = props.get("calendar", {})

        # Upcoming slugs from featured_items
        upcoming_slugs: list[str] = []
        for item in props.get("featured_items", []):
            slug = (item.get("event") or {}).get("url")
            if slug:
                upcoming_slugs.append(slug)

        # Past slugs — click Past tab then collect links
        past_slugs: list[str] = []
        try:
            past_btn = await page.query_selector("text=Past")
            if past_btn:
                await past_btn.click()
                await page.wait_for_timeout(2000)
                links = await page.eval_on_selector_all(
                    'a[href^="/"]',
                    'els => els.map(e => e.getAttribute("href"))'
                )
                for link in links:
                    if link and link.strip("/") and "/" not in link.strip("/"):
                        slug = link.strip("/")
                        # Exclude nav slugs we know aren't events
                        if slug not in ("discover", "signin", "home") and \
                                not slug.startswith("ai-signals") and \
                                "?" not in slug:
                            past_slugs.append(slug)
        except Exception as exc:
            log.warning("  Could not scrape past event slugs: %s", exc)

        return calendar, upcoming_slugs, past_slugs

    # ── Event page ────────────────────────────────────────────────────────

    async def _scrape_event(
        self,
        page,
        slug: str,
        group_urlname: str,
        status: str,
        now: datetime,
    ) -> tuple[Optional[EventRaw], Optional[VenueRaw]]:
        url = f"{LUMA_BASE}/{slug}"
        await page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT)
        await page.wait_for_timeout(1000)

        next_data = await self._get_next_data(page)
        props = (next_data.get("props", {})
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

        title = event.get("name", "")
        event_url = f"{LUMA_BASE}/{slug}"
        is_online = event.get("location_type") == "online"

        starts_at = _parse_dt(event.get("start_at"))
        ends_at   = _parse_dt(event.get("end_at"))

        guest_count = props.get("guest_count")
        rsvp_count = int(guest_count) if guest_count is not None else None

        # ── Venue ─────────────────────────────────────────────────────────
        venue_raw: Optional[VenueRaw] = None
        venue_id: Optional[str] = None

        if not is_online:
            geo = event.get("geo_address_info") or {}
            place_id = geo.get("place_id")

            if place_id:
                venue_id = f"luma_{place_id}"
                full_address = None
                localized = geo.get("localized") or {}
                # Try en-GB first, fall back to first available locale
                locale_data = localized.get("en-GB") or next(iter(localized.values()), {})
                full_address = locale_data.get("full_address") or geo.get("address")

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
            scrape_method="playwright_nextdata",
        )

        return event_raw, venue_raw

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _get_next_data(self, page) -> dict:
        el = await page.query_selector("#__NEXT_DATA__")
        if not el:
            return {}
        try:
            return json.loads(await el.text_content())
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