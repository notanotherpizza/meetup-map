"""
shared/models.py
────────────────
Pydantic models for every message type that flows through Kafka.
Using models here means:
  - the seed producer, workers, and sink consumer all speak the same schema
  - validation happens at produce-time, not just at consume-time
  - easy to evolve the schema with Optional fields
"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, HttpUrl


class GroupSeed(BaseModel):
    """
    Published to: groups-to-scrape
    One message per group that needs scraping.
    """
    group_urlname: str           # e.g. "PyData-London"
    group_url: str               # e.g. "https://www.meetup.com/pydata-london/"
    pro_network: str             # e.g. "pydata"
    seeded_at: datetime


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
    lat: Optional[float] = None
    lon: Optional[float] = None
    member_count: Optional[int] = None
    meetup_url: str
    scraped_at: datetime
    scrape_method: str           # "api" or "playwright"


class EventRaw(BaseModel):
    """
    Published to: events-raw
    One message per event, published by a worker after scraping a group.
    """
    event_id: str                # Meetup's own event ID
    group_urlname: str
    title: str
    description: Optional[str] = None
    event_url: str
    status: str                  # "past" | "upcoming" | "cancelled"
    is_online: bool = False
    venue_name: Optional[str] = None
    venue_lat: Optional[float] = None
    venue_lon: Optional[float] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    rsvp_count: Optional[int] = None
    scraped_at: datetime
    scrape_method: str
