"""
worker/platforms/meetup.py
──────────────────────────
Meetup.com platform scraper.

Implements the Platform interface using Meetup's GQL2 API.
No Playwright needed — pure HTTP via httpx.

API notes (as of April 2026):
- groupByUrlname returns null for some Pro Network groups (auth required)
  These are logged as warnings and skipped gracefully.
- filter.status must be an array of EventStatus enums: [ACTIVE, PAST, CANCELLED]
- filter.beforeDateTime expects DateTime type (not String)
- rsvpCount field no longer exists — use going.totalCount
- venueType field no longer exists — use isOnline
- Persisted queries (APQ) must omit the extensions.persistedQuery field entirely
  on retry — Meetup rejects requests with a bad hash even with a full query body.
"""
import asyncio
import logging
from datetime import datetime, timezone

import httpx

from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw
from worker.platforms.base import Platform, ScrapeResult

log = logging.getLogger(__name__)

MEETUP_GQL_URL = "https://www.meetup.com/gql2"

GROUP_HOME_HASH = "012d7194e1b3746c687a04e05cdf39a25e33a7f8228bb3c563ee55432c718bee"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; meetupmap-worker/0.1)",
}

GQL_QUERIES = {
    "groupHome": """
query groupHome($urlname: String!, $includePrivateInfo: Boolean) {
  groupByUrlname(urlname: $urlname) {
    name
    city
    country
    lat
    lon
    stats {
      memberCounts {
        all
      }
    }
  }
}
""",
    "getPastGroupEvents": """
query getPastGroupEvents($urlname: String!, $beforeDateTime: DateTime, $after: String) {
  groupByUrlname(urlname: $urlname) {
    events(
      filter: { beforeDateTime: $beforeDateTime, status: [ACTIVE, PAST, CANCELLED] }
      first: 20
      sort: DESC
      after: $after
    ) {
      totalCount
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id title eventUrl dateTime endTime isOnline
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
    "getUpcomingGroupEvents": """
query getUpcomingGroupEvents($urlname: String!, $afterDateTime: DateTime) {
  groupByUrlname(urlname: $urlname) {
    events(
      filter: { afterDateTime: $afterDateTime, status: [ACTIVE, DRAFT] }
      first: 20
      sort: ASC
    ) {
      edges {
        node {
          id title eventUrl dateTime endTime isOnline
          going { totalCount }
          venue { id name address city state country }
        }
      }
    }
  }
}
""",
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

        if group_home is None:
            log.warning(
                "groupByUrlname returned null for %s — group may require auth "
                "(Pro Network restriction). Writing minimal record.",
                seed.group_urlname,
            )
            now = datetime.now(timezone.utc)
            duration_ms = int((asyncio.get_event_loop().time() - t_start) * 1000)
            group = GroupRaw(
                group_urlname=seed.group_urlname,
                name=seed.name or seed.group_urlname,
                pro_network=seed.pro_network,
                platform="meetup",
                city=seed.city,
                country=seed.country,
                member_count=seed.member_count,
                source_url=seed.group_url,
                scraped_at=now,
                scrape_method="gql2_null",
                events_scrape_ok=False,
                total_past_events=None,
                worker_id=worker_id,
                scrape_duration_ms=duration_ms,
            )
            return ScrapeResult(group=group, venues=[], past_events=[], upcoming_events=[])

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

        # Prefer scraped data over seed data for location fields
        city    = group_home.get("city")    or seed.city
        country = group_home.get("country") or seed.country
        lat     = group_home.get("lat")     or seed.lat
        lon     = group_home.get("lon")     or seed.lon
        name    = group_home.get("name")    or seed.name or seed.group_urlname

        group = GroupRaw(
            group_urlname=seed.group_urlname,
            name=name,
            pro_network=seed.pro_network,
            platform="meetup",
            city=city,
            country=country,
            member_count=member_count,
            lat=lat,
            lon=lon,
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
        hash_: str = "",
    ) -> dict:
        # If we have a valid hash, try APQ first
        if hash_:
            payload = {
                "operationName": operation,
                "variables": variables,
                "extensions": {"persistedQuery": {"version": 1, "sha256Hash": hash_}},
            }
            resp = await client.post(MEETUP_GQL_URL, json=payload, headers=HEADERS)
            resp.raise_for_status()
            data = resp.json()

            errors = data.get("errors", [])
            apq_miss = any(
                e.get("extensions", {}).get("classification") in (
                    "PersistedQueryNotFound", "PersistedQueryIdInvalid"
                ) for e in errors
            )
            if not apq_miss:
                if "errors" in data:
                    raise ValueError(f"GQL errors in {operation}: {data['errors']}")
                return data

            log.debug("APQ miss for %s — retrying with full query, no hash", operation)

        # Send full query without persistedQuery extension
        if operation not in GQL_QUERIES:
            raise ValueError(f"No query body for operation: {operation}")

        retry_payload = {
            "operationName": operation,
            "variables": variables,
            "query": GQL_QUERIES[operation],
        }
        resp = await client.post(MEETUP_GQL_URL, json=retry_payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise ValueError(f"GQL errors in {operation}: {data['errors']}")
        return data

    async def _fetch_group_home(
        self,
        urlname: str,
        client: httpx.AsyncClient,
    ) -> dict | None:
        """Returns group home data, or None if groupByUrlname returned null."""
        try:
            data = await self._gql(
                client, "groupHome",
                {"urlname": urlname, "includePrivateInfo": False},
                GROUP_HOME_HASH,
            )
            return data.get("data", {}).get("groupByUrlname")
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
            data = await self._gql(client, "getPastGroupEvents", variables)
            group_data = data.get("data", {}).get("groupByUrlname")

            if group_data is None:
                raise ValueError(f"groupByUrlname returned null for {urlname}")

            events_data = group_data.get("events", {})

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
        )
        group_data = data.get("data", {}).get("groupByUrlname")
        if group_data:
            edges = group_data.get("events", {}).get("edges", [])
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
                starts_at = datetime.fromisoformat(
                    event["dateTime"].replace("Z", "+00:00")
                )
            except Exception:
                pass

        ends_at = None
        if "endTime" in event:
            try:
                ends_at = datetime.fromisoformat(
                    event["endTime"].replace("Z", "+00:00")
                )
            except Exception:
                pass

        venue = event.get("venue") or {}
        venue_id = str(venue.get("id", "")).strip() or None
        is_online = (
            event.get("isOnline", False)
            or "online" in (venue.get("name") or "").lower()
        )
        if is_online:
            venue_id = None

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
            ends_at=ends_at,
            rsvp_count=rsvp_count,
            scraped_at=now,
            scrape_method="gql2",
        )