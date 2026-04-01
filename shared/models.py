"""
shared/models.py
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class GroupSeed(BaseModel):
    """Published to: groups-to-scrape"""
    group_urlname: str
    group_url: str
    pro_network: str
    seeded_at: datetime
    name: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    member_count: Optional[int] = None

class GroupRaw(BaseModel):
    """Published to: groups-raw"""
    group_urlname: str
    name: str
    pro_network: str
    city: Optional[str] = None
    country: Optional[str] = None
    member_count: Optional[int] = None
    meetup_url: str
    scraped_at: datetime
    scrape_method: str
    total_past_events: Optional[int] = None
    events_scrape_ok: bool = False
    worker_id: str
    scrape_duration_ms: int

class VenueRaw(BaseModel):
    """Published to: venues-raw"""
    venue_id: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    scraped_at: datetime

class EventRaw(BaseModel):
    """Published to: events-raw"""
    event_id: str
    group_urlname: str
    title: str
    event_url: str
    status: str
    is_online: bool = False
    venue_id: Optional[str] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    rsvp_count: Optional[int] = None
    scraped_at: datetime
    scrape_method: str