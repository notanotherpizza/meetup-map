"""
worker/platforms/meetup.py
──────────────────────────
Meetup.com platform scraper.

Implements the Platform interface using Meetup's GQL2 persisted query API.
No Playwright needed — pure HTTP via httpx.

Logic extracted from the original worker/scraper.py with no behavioural changes.
"""
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw
from worker.platforms.base import Platform, ScrapeResult

log = logging.getLogger(__name__)

MEETUP_GQL_URL = "https://www.meetup.com/gql2"

GROUP_HOME_HASH      = "012d7194e1b3746c687a04e05cdf39a25e33a7f8228bb3c563ee55432c718bee"
# Queries below include rsvpCount which changes the hash — use empty strings
# so the APQ retry path always fires and sends the full query body.
PAST_EVENTS_HASH     = ""
UPCOMING_EVENTS_HASH = ""

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-worker/0.1)",
}

GQL_QUERIES = {
    "groupHome": """
query groupHome($urlname: String!, $includePrivateInfo: Boolean) {
  groupByUrlname(urlname: $urlname) {
    stats {
      memberCounts {
        all
      }
    }
  }
}
""",
    "getPastGroupEvents": """
query getPastGroupEvents($urlname: String!, $beforeDateTime: String, $after: String) {
  groupByUrlname(urlname: $urlname) {
    events(input: {startDateBefore: $beforeDateTime}, after: $after, first: 20) {
      totalCount
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id title eventUrl dateTime isOnline venueType
          rsvpCount
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
    "getUpcomingGroupEvents": """
query getUpcomingGroupEvents($urlname: String!, $afterDateTime: String) {
  groupByUrlname(urlname: $urlname) {
    events(input: {startDateAfter: $afterDateTime}, first: 20) {
      edges {
        node {
          id title eventUrl dateTime isOnline venueType
          rsvpCount
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
}

HASHES = {
    "groupHome": GROUP_HOME_HASH,
    "getPastGroupEvents": PAST_EVENTS_HASH,
    "getUpcomingGroupEvents": UPCOMING_EVENTS_HASH,
}


class MeetupPlatform(Platform):

    def can_handle(self, url: str) -> bool:
        return "meetup.com" in url

    async def scrape(
        self,
        seed: GroupSeed,
        browser,
        http_client: httpx.AsyncClient,
        max_past_events: int,
        worker_id: str,
    ) -> ScrapeResult:
        t_start = asyncio.get_event_loop().time()
        events_scrape_ok = False
        past: list[dict] = []
        upcoming: list[dict] = []
        group_home: dict = {}
        total_past_count: int | None = None

        try:
            group_home, (past, upcoming, total_past_count) = await asyncio.gather(
                self._fetch_group_home(seed.group_urlname, http_client),
                self._fetch_events(seed.group_urlname, http_client, max_past_events),
            )
            events_scrape_ok = True
        except Exception as exc:
            log.warning("Events fetch failed for %s: %s — writing group record anyway",
                        seed.group_urlname, exc)
            try:
                group_home = await self._fetch_group_home(seed.group_urlname, http_client)
            except Exception:
                group_home = {}

        log.info("  -> %d/%s past, %d upcoming | members: %s | events_ok: %s",
                 len(past),
                 total_past_count if total_past_count is not None else "?",
                 len(upcoming),
                 (group_home.get("stats") or {}).get("memberCounts", {}).get("all", "?"),
                 events_scrape_ok)

        now = datetime.now(timezone.utc)
        duration_ms = int((asyncio.get_event_loop().time() - t_start) * 1000)

        member_count = (
            (group_home.get("stats") or {})
            .get("memberCounts", {})
            .get("all")
        )

        group = GroupRaw(
            group_urlname=seed.group_urlname,
            name=seed.name or seed.group_urlname,
            pro_network=seed.pro_network,
            platform="meetup",
            city=seed.city,
            country=seed.country,
            member_count=member_count,
            source_url=seed.group_url,
            scraped_at=now,
            scrape_method="gql2",
            events_scrape_ok=events_scrape_ok,
            total_past_events=total_past_count,
            worker_id=worker_id,
            scrape_duration_ms=duration_ms,
        )

        venues: list[VenueRaw] = []
        past_events: list[EventRaw] = []
        upcoming_events: list[EventRaw] = []
        seen_venues: set[str] = set()

        for event in past + upcoming:
            status = "past" if event in past else "upcoming"
            venue = event.get("venue") or {}
            venue_id = str(venue.get("id", "")).strip()
            is_online = (
                event.get("isOnline", False)
                or event.get("venueType") == "ONLINE"
                or "online" in (venue.get("name") or "").lower()
            )

            if venue_id and not is_online and venue_id not in seen_venues:
                vr = self._build_venue(venue, now)
                if vr:
                    venues.append(vr)
                    seen_venues.add(venue_id)

            er = self._build_event(seed, event, status, now)
            if er:
                if status == "past":
                    past_events.append(er)
                else:
                    upcoming_events.append(er)

        return ScrapeResult(
            group=group,
            venues=venues,
            past_events=past_events,
            upcoming_events=upcoming_events,
        )

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _gql(
        self,
        client: httpx.AsyncClient,
        operation: str,
        variables: dict,
        hash_: str,
    ) -> dict:
        payload = {
            "operationName": operation,
            "variables": variables,
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": hash_}},
        }
        resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        errors = data.get("errors", [])
        if any(e.get("extensions", {}).get("classification") == "PersistedQueryNotFound"
               for e in errors):
            log.debug("PersistedQueryNotFound for %s — retrying with full query", operation)
            if operation in GQL_QUERIES:
                payload["query"] = GQL_QUERIES[operation]
                resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
                resp.raise_for_status()
                data = resp.json()

        if "errors" in data:
            raise ValueError(f"GQL errors in {operation}: {data['errors']}")
        return data

    async def _fetch_group_home(
        self,
        urlname: str,
        client: httpx.AsyncClient,
    ) -> dict:
        try:
            data = await self._gql(
                client, "groupHome",
                {"urlname": urlname, "includePrivateInfo": False},
                GROUP_HOME_HASH,
            )
            return data.get("data", {}).get("groupByUrlname", {}) or {}
        except Exception as exc:
            log.warning("groupHome failed for %s: %s", urlname, exc)
            return {}

    async def _fetch_events(
        self,
        urlname: str,
        client: httpx.AsyncClient,
        max_events: int,
    ) -> tuple[list[dict], list[dict], int | None]:
        now = datetime.now(timezone.utc).isoformat()
        past: list[dict] = []
        upcoming: list[dict] = []
        total_past_count: int | None = None
        cursor = None

        while max_events == 0 or len(past) < max_events:
            variables = {"urlname": urlname, "beforeDateTime": now}
            if cursor:
                variables["after"] = cursor
            data = await self._gql(client, "getPastGroupEvents", variables, PAST_EVENTS_HASH)
            events_data = (data.get("data", {})
                           .get("groupByUrlname", {})
                           .get("events", {}))

            if total_past_count is None:
                total_past_count = events_data.get("totalCount")

            edges = events_data.get("edges", [])
            past.extend(edge["node"] for edge in edges if "node" in edge)
            page_info = events_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        data = await self._gql(
            client, "getUpcomingGroupEvents",
            {"urlname": urlname, "afterDateTime": now},
            UPCOMING_EVENTS_HASH,
        )
        edges = (data.get("data", {})
                     .get("groupByUrlname", {})
                     .get("events", {})
                     .get("edges", []))
        upcoming.extend(edge["node"] for edge in edges if "node" in edge)

        capped_past = past if max_events == 0 else past[:max_events]
        return capped_past, upcoming, total_past_count

    def _build_venue(self, venue: dict, now: datetime) -> VenueRaw | None:
        venue_id = str(venue.get("id", "")).strip()
        if not venue_id:
            return None
        return VenueRaw(
            venue_id=venue_id,
            name=venue.get("name") or None,
            address=venue.get("address") or None,
            city=venue.get("city") or None,
            state=venue.get("state") or None,
            country=venue.get("country") or None,
            scraped_at=now,
        )

    def _build_event(
        self,
        seed: GroupSeed,
        event: dict,
        status: str,
        now: datetime,
    ) -> EventRaw | None:
        event_id = str(event.get("id", ""))
        if not event_id:
            return None

        starts_at = None
        if "dateTime" in event:
            try:
                starts_at = datetime.fromisoformat(event["dateTime"])
            except Exception:
                pass

        venue = event.get("venue") or {}
        venue_id = str(venue.get("id", "")).strip() or None
        is_online = (
            event.get("isOnline", False)
            or event.get("venueType") == "ONLINE"
            or "online" in (venue.get("name") or "").lower()
        )
        if is_online:
            venue_id = None

        # rsvpCount matches what Meetup displays on the event page.
        # going.totalCount only counts confirmed "Going" clicks — rsvpCount
        # includes all attendee types and matches the displayed headcount.
        rsvp_count = event.get("rsvpCount")
        if rsvp_count is None:
            going = event.get("going") or {}
            rsvp_count = going.get("totalCount") if isinstance(going, dict) else None

        return EventRaw(
            event_id=event_id,
            group_urlname=seed.group_urlname,
            title=event.get("title", ""),
            event_url=event.get("eventUrl", ""),
            status=status,
            is_online=is_online,
            venue_id=venue_id,
            starts_at=starts_at,
            rsvp_count=rsvp_count,
            scraped_at=now,
            scrape_method="gql2",
        )