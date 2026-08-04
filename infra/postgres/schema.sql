-- infra/postgres/schema.sql
-- Applied idempotently at startup by shared.db.ensure_schema(). Column names
-- match shared/models.py fields exactly (group_urlname, venue_id, event_id)
-- so the download API's public field names never change.

CREATE TABLE IF NOT EXISTS groups (
    group_urlname       TEXT        PRIMARY KEY,
    name                TEXT        NOT NULL,
    pro_network         TEXT        NOT NULL,
    platform            TEXT        NOT NULL DEFAULT 'meetup',
    city                TEXT,
    country             TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    member_count        INT,
    source_url          TEXT        NOT NULL,
    description         TEXT,
    total_past_events   INT,
    events_scrape_ok    BOOLEAN     NOT NULL DEFAULT false,
    scrape_method       TEXT,
    worker_id           TEXT,
    scrape_duration_ms  INT,
    scraped_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS venues (
    venue_id        TEXT            PRIMARY KEY,
    name            TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    geocode_source  TEXT,
    scraped_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT        PRIMARY KEY,
    group_urlname   TEXT        NOT NULL REFERENCES groups(group_urlname) ON DELETE CASCADE,
    title           TEXT        NOT NULL,
    event_url       TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    is_online       BOOLEAN     NOT NULL DEFAULT false,
    venue_id        TEXT        REFERENCES venues(venue_id),
    starts_at       TIMESTAMPTZ,
    ends_at         TIMESTAMPTZ,
    rsvp_count      INT,
    description     TEXT,
    scrape_method   TEXT,
    scraped_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Keep updated_at fresh on every upsert
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

CREATE INDEX IF NOT EXISTS events_group_urlname_idx ON events (group_urlname);
CREATE INDEX IF NOT EXISTS events_starts_at_idx      ON events (starts_at DESC);
CREATE INDEX IF NOT EXISTS events_status_idx         ON events (status);
CREATE INDEX IF NOT EXISTS events_venue_id_idx       ON events (venue_id);
CREATE INDEX IF NOT EXISTS events_group_starts_idx   ON events (group_urlname, starts_at DESC);
CREATE INDEX IF NOT EXISTS groups_pro_network_idx    ON groups (pro_network);
CREATE INDEX IF NOT EXISTS groups_platform_idx       ON groups (platform);
CREATE INDEX IF NOT EXISTS venues_country_idx        ON venues (country);
