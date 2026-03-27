"""
sink/consumer.py
────────────────
Sink consumer: reads GroupRaw and EventRaw messages from Kafka and upserts
them into Postgres. Uses ON CONFLICT DO UPDATE so it's safe to re-run and
handles duplicate messages from at-least-once delivery.

Usage:
    python -m sink.consumer
"""
import json
import os
import logging
import sys
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from shared.kafka_client import make_consumer
from shared.models import EventRaw, GroupRaw
from shared.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

# ── SQL ───────────────────────────────────────────────────────────────────────

UPSERT_GROUP = """
INSERT INTO groups (
    id, name, pro_network, city, country, lat, lon,
    member_count, meetup_url, last_scraped_at
) VALUES (
    %(group_urlname)s, %(name)s, %(pro_network)s, %(city)s, %(country)s,
    %(lat)s, %(lon)s, %(member_count)s, %(meetup_url)s, %(scraped_at)s
)
ON CONFLICT (id) DO UPDATE SET
    name            = EXCLUDED.name,
    city            = EXCLUDED.city,
    country         = EXCLUDED.country,
    lat             = EXCLUDED.lat,
    lon             = EXCLUDED.lon,
    member_count    = EXCLUDED.member_count,
    meetup_url      = EXCLUDED.meetup_url,
    last_scraped_at = EXCLUDED.last_scraped_at,
    updated_at      = now()
"""

UPSERT_EVENT = """
INSERT INTO events (
    id, group_id, title, description, event_url, status,
    is_online, venue_name, venue_lat, venue_lon,
    starts_at, ends_at, rsvp_count, last_scraped_at
) VALUES (
    %(event_id)s, %(group_urlname)s, %(title)s, %(description)s,
    %(event_url)s, %(status)s, %(is_online)s, %(venue_name)s,
    %(venue_lat)s, %(venue_lon)s, %(starts_at)s, %(ends_at)s,
    %(rsvp_count)s, %(scraped_at)s
)
ON CONFLICT (id) DO UPDATE SET
    title           = EXCLUDED.title,
    description     = EXCLUDED.description,
    status          = EXCLUDED.status,
    is_online       = EXCLUDED.is_online,
    venue_name      = EXCLUDED.venue_name,
    venue_lat       = EXCLUDED.venue_lat,
    venue_lon       = EXCLUDED.venue_lon,
    starts_at       = EXCLUDED.starts_at,
    rsvp_count      = EXCLUDED.rsvp_count,
    last_scraped_at = EXCLUDED.last_scraped_at,
    updated_at      = now()
"""

# ── Handlers ──────────────────────────────────────────────────────────────────

def handle_group(payload: dict, conn: psycopg.Connection) -> None:
    group = GroupRaw(**payload)
    with conn.cursor() as cur:
        cur.execute(UPSERT_GROUP, group.model_dump(mode="json"))
    conn.commit()
    log.debug("Upserted group: %s", group.group_urlname)


def handle_event(payload: dict, conn: psycopg.Connection) -> None:
    event = EventRaw(**payload)
    # Events reference groups — if the group hasn't arrived yet this will
    # fail the FK constraint. We log and skip; the message won't be committed
    # so it will be redelivered and retried.
    with conn.cursor() as cur:
        cur.execute(UPSERT_EVENT, event.model_dump(mode="json"))
    conn.commit()
    log.debug("Upserted event: %s / %s", event.group_urlname, event.event_id)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(settings: Settings) -> None:
    # Single consumer subscribed to both raw topics
    consumer = make_consumer(
        settings,
        group_id="meetupmap-sink",
        topics=[settings.topic_groups_raw, settings.topic_events_raw],
    )

    log.info(
        "Sink started. Listening on '%s' and '%s'…",
        settings.topic_groups_raw,
        settings.topic_events_raw,
    )

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 3  # 3 × 5s silence = both topics drained

    groups_written = 0
    events_written = 0

    with psycopg.connect(settings.postgres_uri, row_factory=dict_row) as conn:
        try:
            while True:
                msg = consumer.poll(timeout=5.0)

                if msg is None:
                    if drain_mode:
                        empty_polls += 1
                        log.info("No messages (%d/%d)… (groups: %d, events: %d)",
                                 empty_polls, empty_polls_needed,
                                 groups_written, events_written)
                        if empty_polls >= empty_polls_needed:
                            log.info("Topics drained — exiting.")
                            break
                    elif groups_written or events_written:
                        log.info("Waiting… (groups: %d, events: %d written so far)",
                                 groups_written, events_written)
                    continue

                empty_polls = 0  # reset on any message

                if msg.error():
                    log.error("Kafka error: %s", msg.error())
                    continue

                topic = msg.topic()
                try:
                    payload = json.loads(msg.value())

                    if topic == settings.topic_groups_raw:
                        handle_group(payload, conn)
                        groups_written += 1

                    elif topic == settings.topic_events_raw:
                        handle_event(payload, conn)
                        events_written += 1

                    consumer.commit(msg)

                except psycopg.errors.ForeignKeyViolation:
                    # Group not yet written — don't commit, will retry
                    conn.rollback()
                    log.warning(
                        "FK violation for event %s — group not yet written, will retry",
                        msg.key(),
                    )
                except Exception as exc:
                    conn.rollback()
                    log.error("Failed to sink %s: %s", msg.key(), exc, exc_info=True)

        except KeyboardInterrupt:
            log.info(
                "Shutting down. Final counts — groups: %d, events: %d",
                groups_written, events_written,
            )
        finally:
            consumer.close()


def main() -> None:
    settings = Settings.from_env()
    try:
        run(settings)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()