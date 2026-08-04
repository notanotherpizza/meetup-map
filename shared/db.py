"""
shared/db.py
────────────
Postgres connection + upsert helpers used by the scraper and batch runner.
Replaces shared/iceberg.py.
"""
from pathlib import Path

import psycopg

from shared.settings import Settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "infra" / "postgres" / "schema.sql"

GROUP_UPSERT = """
INSERT INTO groups (
    group_urlname, name, pro_network, platform, city, country, lat, lon,
    member_count, source_url, description, total_past_events,
    events_scrape_ok, scrape_method, worker_id, scrape_duration_ms, scraped_at
) VALUES (
    %(group_urlname)s, %(name)s, %(pro_network)s, %(platform)s, %(city)s,
    %(country)s, %(lat)s, %(lon)s, %(member_count)s, %(source_url)s,
    %(description)s, %(total_past_events)s, %(events_scrape_ok)s,
    %(scrape_method)s, %(worker_id)s, %(scrape_duration_ms)s, %(scraped_at)s
)
ON CONFLICT (group_urlname) DO UPDATE SET
    name = EXCLUDED.name, pro_network = EXCLUDED.pro_network,
    platform = EXCLUDED.platform, city = EXCLUDED.city, country = EXCLUDED.country,
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, member_count = EXCLUDED.member_count,
    source_url = EXCLUDED.source_url, description = EXCLUDED.description,
    total_past_events = EXCLUDED.total_past_events,
    events_scrape_ok = EXCLUDED.events_scrape_ok, scrape_method = EXCLUDED.scrape_method,
    worker_id = EXCLUDED.worker_id, scrape_duration_ms = EXCLUDED.scrape_duration_ms,
    scraped_at = EXCLUDED.scraped_at
"""

VENUE_UPSERT = """
INSERT INTO venues (
    venue_id, name, address, city, state, country, lat, lon,
    geocode_source, scraped_at
) VALUES (
    %(venue_id)s, %(name)s, %(address)s, %(city)s, %(state)s, %(country)s,
    %(lat)s, %(lon)s, %(geocode_source)s, %(scraped_at)s
)
ON CONFLICT (venue_id) DO UPDATE SET
    name = EXCLUDED.name, address = EXCLUDED.address, city = EXCLUDED.city,
    state = EXCLUDED.state, country = EXCLUDED.country, lat = EXCLUDED.lat,
    lon = EXCLUDED.lon, geocode_source = EXCLUDED.geocode_source,
    scraped_at = EXCLUDED.scraped_at
"""

EVENT_UPSERT = """
INSERT INTO events (
    event_id, group_urlname, title, event_url, status, is_online,
    venue_id, starts_at, ends_at, rsvp_count, description, scrape_method, scraped_at
) VALUES (
    %(event_id)s, %(group_urlname)s, %(title)s, %(event_url)s, %(status)s,
    %(is_online)s, %(venue_id)s, %(starts_at)s, %(ends_at)s, %(rsvp_count)s,
    %(description)s, %(scrape_method)s, %(scraped_at)s
)
ON CONFLICT (event_id) DO UPDATE SET
    title = EXCLUDED.title, event_url = EXCLUDED.event_url, status = EXCLUDED.status,
    is_online = EXCLUDED.is_online, venue_id = EXCLUDED.venue_id,
    starts_at = EXCLUDED.starts_at, ends_at = EXCLUDED.ends_at,
    rsvp_count = EXCLUDED.rsvp_count, description = EXCLUDED.description,
    scrape_method = EXCLUDED.scrape_method, scraped_at = EXCLUDED.scraped_at
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_PATH.read_text())


def connect(settings: Settings) -> psycopg.Connection:
    conn = psycopg.connect(settings.postgres_uri, autocommit=True)
    ensure_schema(conn)
    return conn


def write_result(conn: psycopg.Connection, result) -> None:
    """Upsert a ScrapeResult's group, venues, and events into Postgres.

    Order matters: venues and events carry foreign keys back to groups/venues.
    """
    with conn.transaction():
        conn.execute(GROUP_UPSERT, result.group.model_dump())
        for v in result.venues:
            conn.execute(VENUE_UPSERT, v.model_dump())
        for e in result.past_events + result.upcoming_events:
            conn.execute(EVENT_UPSERT, e.model_dump())
