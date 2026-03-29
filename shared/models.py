"""
shared/models.py
────────────────
Pydantic models for every message type that flows through Kafka.
"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupSeed(BaseModel):
    """
    Published to: groups-to-scrape
    Carries all metadata available at seed time so workers are fully stateless —
    they don't need to re-fetch group metadata from the Meetup API.
    """
    group_urlname: str
    group_url: str
    pro_network: str
    seeded_at: datetime
    # Metadata from the seed API call — workers use these directly
    name: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None          # Meetup truncated coords (2dp)
    lon: Optional[float] = None
    member_count: Optional[int] = None


class GroupRaw(BaseModel):
    """
    Published to: groups-raw
    Full metadata about a group, as scraped by a worker.
    """
    group_urlname: str
    name: str
    pro_network: str
    city: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None          # Nominatim-geocoded coords (4dp)
    lon: Optional[float] = None
    member_count: Optional[int] = None
    meetup_url: str
    scraped_at: datetime
    scrape_method: str


class EventRaw(BaseModel):
    """
    Published to: events-raw
    One message per event, published by a worker after scraping a group.
    """
    event_id: str
    group_urlname: str
    title: str
    event_url: str
    status: str                          # "past" | "upcoming" | "cancelled"
    is_online: bool = False
    venue_name: Optional[str] = None
    venue_lat: Optional[float] = None
    venue_lon: Optional[float] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    rsvp_count: Optional[int] = None
    scraped_at: datetime
    scrape_method: str