"""
worker/scraper.py
─────────────────
Stateless worker: consumes GroupSeed messages from Kafka, routes each seed
to the appropriate platform scraper, and publishes GroupRaw + VenueRaw +
EventRaw messages.

No Postgres dependency — all geocoding and persistence handled by the sink.

Platform routing:
  - seed.platform == "meetup" → worker.platforms.meetup.MeetupPlatform
  - seed.platform == "luma"   → worker.platforms.luma.LumaPlatform

Usage:
    python -m worker.scraper
"""
import asyncio
import json
import logging
import os
import sys

import httpx
from playwright.async_api import async_playwright

from shared.kafka_client import make_consumer, make_producer, publish
from shared.models import GroupSeed
from shared.settings import Settings
from worker.platforms.base import ScrapeResult
from worker.platforms.luma import LumaPlatform
from worker.platforms.meetup import MeetupPlatform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

PLATFORMS = [
    MeetupPlatform(),
    LumaPlatform(),
]


def get_platform(seed: GroupSeed):
    """Return the first platform that can handle this seed's URL."""
    # Prefer explicit platform field, fall back to URL sniffing
    if seed.platform == "luma":
        return LumaPlatform()
    if seed.platform == "meetup":
        return MeetupPlatform()
    # Fallback: sniff from URL
    for p in PLATFORMS:
        if p.can_handle(seed.group_url):
            return p
    raise ValueError(
        f"No platform handler for seed {seed.group_urlname!r} "
        f"(platform={seed.platform!r}, url={seed.group_url!r})"
    )


def publish_result(
    result: ScrapeResult,
    producer,
    settings: Settings,
) -> None:
    """Publish a ScrapeResult to the appropriate Kafka topics."""
    # Group
    publish(
        producer,
        topic=settings.topic_groups_raw,
        value=result.group.model_dump(mode="json"),
        key=result.group.group_urlname,
    )

    # Venues (deduplicated by caller — each venue published once)
    for venue in result.venues:
        publish(
            producer,
            topic=settings.topic_venues_raw,
            value=venue.model_dump(mode="json"),
            key=venue.venue_id,
        )

    # Events
    for event in result.past_events + result.upcoming_events:
        publish(
            producer,
            topic=settings.topic_events_raw,
            value=event.model_dump(mode="json"),
            key=f"{result.group.group_urlname}:{event.event_id}",
        )

    log.info(
        "  -> Published 1 group + %d venues + %d events for %s",
        len(result.venues),
        len(result.past_events) + len(result.upcoming_events),
        result.group.group_urlname,
    )


async def process_seed(
    seed: GroupSeed,
    producer,
    settings: Settings,
    http_client: httpx.AsyncClient,
    browser,
) -> None:
    log.info("Processing: %s (platform=%s)", seed.group_urlname, seed.platform)

    worker_id = os.environ.get("WORKER_ID", __import__("socket").gethostname())
    platform = get_platform(seed)

    try:
        result = await platform.scrape(
            seed=seed,
            browser=browser,
            http_client=http_client,
            max_past_events=settings.max_events_per_group,
            worker_id=worker_id,
        )
        publish_result(result, producer, settings)
        producer.flush(timeout=10)
    except Exception as exc:
        log.error("Failed to process %s: %s", seed.group_urlname, exc, exc_info=True)


async def run(settings: Settings) -> None:
    consumer = make_consumer(
        settings,
        group_id="meetupmap-workers",
        topics=[settings.topic_groups_to_scrape],
    )
    producer = make_producer(settings)
    log.info("Worker started. Listening on '%s'...", settings.topic_groups_to_scrape)

    drain_mode = os.environ.get("DRAIN_MODE", "").lower() == "true"
    empty_polls = 0
    empty_polls_needed = 3

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        async with httpx.AsyncClient(timeout=30) as http_client:
            try:
                while True:
                    msg = consumer.poll(timeout=5.0)
                    if msg is None:
                        if drain_mode:
                            empty_polls += 1
                            log.info(
                                "No messages (%d/%d)...",
                                empty_polls, empty_polls_needed,
                            )
                            if empty_polls >= empty_polls_needed:
                                log.info("Topic drained — exiting.")
                                break
                        continue

                    empty_polls = 0

                    if msg.error():
                        log.error("Kafka error: %s", msg.error())
                        continue

                    try:
                        seed = GroupSeed(**json.loads(msg.value()))
                        await process_seed(
                            seed, producer, settings, http_client, browser
                        )
                        consumer.commit(msg)
                        await asyncio.sleep(settings.request_delay_seconds)
                    except Exception as exc:
                        log.error(
                            "Failed to process %s: %s",
                            msg.key(), exc, exc_info=True,
                        )
            except KeyboardInterrupt:
                log.info("Shutting down...")
            finally:
                await browser.close()
                consumer.close()


def main() -> None:
    settings = Settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()