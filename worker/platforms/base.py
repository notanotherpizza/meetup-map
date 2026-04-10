"""
worker/platforms/base.py
────────────────────────
Abstract base class that all platform scrapers must implement.

Each platform module (meetup.py, luma.py) provides a single class that
inherits from Platform and implements the three methods below.

The worker dispatcher (scraper.py) calls:
  1. platform.can_handle(url)  — to route a GroupSeed to the right module
  2. platform.scrape_group()   — to fetch group metadata
  3. platform.scrape_events()  — to fetch past + upcoming events

All platform implementations must be stateless — no Postgres access.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx


from shared.models import EventRaw, GroupRaw, GroupSeed, VenueRaw


@dataclass
class ScrapeResult:
    """
    Everything a platform scrape produces for one group.
    Passed back to scraper.py which publishes it to Kafka.
    """
    group: GroupRaw
    venues: list[VenueRaw]
    past_events: list[EventRaw]
    upcoming_events: list[EventRaw]


class Platform(ABC):
    """Abstract base for all platform scrapers."""

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """Return True if this platform can scrape the given URL."""
        ...

    @abstractmethod
    async def scrape(
        self,
        seed: GroupSeed,
        http_client: httpx.AsyncClient,
        max_past_events: int,
        worker_id: str,
    ) -> ScrapeResult:
        """
        Scrape a group and all its events.

        Args:
            seed:            The GroupSeed message consumed from Kafka.
            browser:         A shared Playwright browser instance (may be unused
                             by HTTP-only platforms).
            http_client:     A shared httpx.AsyncClient (may be unused by
                             Playwright-only platforms).
            max_past_events: Cap on past events to fetch (0 = unlimited).
            worker_id:       Hostname/identifier for scrape_log telemetry.

        Returns:
            ScrapeResult with group, venues, and past + upcoming events.
        """
        ...