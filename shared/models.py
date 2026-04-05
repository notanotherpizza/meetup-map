"""
shared/models.py
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class GroupSeed(BaseModel):
    """Published to: groups-to-scrape"""
    group_urlname: str
    group_url: str
    pro_network: str
    seeded_at: datetime
    platform: str = "meetup"          # "meetup" | "luma"
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
    platform: str = "meetup"          # "meetup" | "luma"
    city: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    member_count: Optional[int] = None
    source_url: str                   # was meetup_url
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
    lat: Optional[float] = None       # pre-geocoded (Luma provides this via Google place_id)
    lon: Optional[float] = None       # pre-geocoded (Luma provides this via Google place_id)
    geocode_source: Optional[str] = None  # 'luma_google' | 'postcode' | 'address' | 'city' | None
    scraped_at: datetime


class EventRaw(BaseModel):
    """Published to: events-raw"""
    event_id: str
    group_urlname: str
    title: str
    event_url: str
    status: str                       # "past" | "upcoming" | "cancelled"
    is_online: bool = False
    venue_id: Optional[str] = None
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    rsvp_count: Optional[int] = None
    scraped_at: datetime
    scrape_method: str