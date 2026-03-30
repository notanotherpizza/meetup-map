-- infra/postgres/schema.sql
-- Idempotent — safe to run multiple times.
-- Run against your Aiven Postgres with:
--   psql $POSTGRES_URI -f infra/postgres/schema.sql

CREATE TABLE IF NOT EXISTS groups (
    id                  TEXT        PRIMARY KEY,  -- group urlname, e.g. "PyData-London"
    name                TEXT        NOT NULL,
    pro_network         TEXT        NOT NULL,     -- "pydata", extensible to others
    city                TEXT,
    country             TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    member_count        INT,
    meetup_url          TEXT,
    last_scraped_at     TIMESTAMPTZ,
    events_scraped_at   TIMESTAMPTZ,             -- SET only when events fetch succeeded
    total_past_events   INT,                     -- from GQL totalCount, accurate even with fetch cap
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS venues (
    id              TEXT            PRIMARY KEY,  -- Meetup's venue ID
    name            TEXT,                         -- raw name from Meetup (often a postcode)
    address         TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    lat             DOUBLE PRECISION,             -- geocoded
    lon             DOUBLE PRECISION,             -- geocoded
    geocode_source  TEXT,                         -- 'postcode' | 'address' | 'city' | 'miss'
    geocode_query   TEXT,                         -- what was sent to Nominatim
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id                TEXT        PRIMARY KEY,  -- Meetup's own event ID
    group_id          TEXT        NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    title             TEXT        NOT NULL,
    event_url         TEXT,
    status            TEXT,                     -- 'past' | 'upcoming' | 'cancelled'
    is_online         BOOLEAN     NOT NULL DEFAULT false,
    venue_id          TEXT        REFERENCES venues(id),
    starts_at         TIMESTAMPTZ,
    ends_at           TIMESTAMPTZ,
    rsvp_count        INT,
    last_scraped_at   TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Geocoding cache — shared across all workers, keyed by "city, Country Name".
-- Workers write here after a Nominatim lookup so subsequent workers and runs
-- get an instant Postgres hit instead of calling Nominatim again.
-- Also stores bounding boxes for place-name URL parameter framing in the map.
CREATE TABLE IF NOT EXISTS geocode_cache (
    query             TEXT        PRIMARY KEY,  -- e.g. "London, United Kingdom"
    lat               DOUBLE PRECISION,         -- null = confirmed miss (city not found)
    lon               DOUBLE PRECISION,
    display_name      TEXT,
    bbox_min_lat      DOUBLE PRECISION,         -- bounding box for map framing
    bbox_max_lat      DOUBLE PRECISION,
    bbox_min_lon      DOUBLE PRECISION,
    bbox_max_lon      DOUBLE PRECISION,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Scrape run tracking
CREATE TABLE IF NOT EXISTS scrape_runs (
    id          SERIAL      PRIMARY KEY,
    networks    TEXT,                           -- comma-separated list or "ALL"
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id              SERIAL      PRIMARY KEY,
    run_id          INT         REFERENCES scrape_runs(id),
    worker_id       TEXT,
    group_id        TEXT        REFERENCES groups(id),
    pro_network     TEXT,
    events_scraped  INT,
    duration_ms     INT,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Triggers to keep updated_at fresh
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DO $$ BEGIN
    CREATE TRIGGER groups_updated_at
        BEFORE UPDATE ON groups
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TRIGGER venues_updated_at
        BEFORE UPDATE ON venues
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TRIGGER events_updated_at
        BEFORE UPDATE ON events
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Indexes
CREATE INDEX IF NOT EXISTS events_group_id_idx    ON events (group_id);
CREATE INDEX IF NOT EXISTS events_starts_at_idx   ON events (starts_at DESC);
CREATE INDEX IF NOT EXISTS events_status_idx      ON events (status);
CREATE INDEX IF NOT EXISTS events_venue_id_idx    ON events (venue_id);
CREATE INDEX IF NOT EXISTS groups_pro_network_idx ON groups (pro_network);
CREATE INDEX IF NOT EXISTS venues_country_idx     ON venues (country);