"""
shared/kafka_client.py
──────────────────────
Thin wrappers around confluent-kafka Producer and Consumer.
Handles SSL config, serialisation (JSON), and delivery callbacks.
All components import make_producer / make_consumer from here.
"""
import json
import logging
from typing import Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared.settings import Settings

log = logging.getLogger(__name__)


# ── Producer ──────────────────────────────────────────────────────────────────

def make_producer(settings: Settings) -> Producer:
    cfg = settings.kafka_ssl_config() | {
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }
    return Producer(cfg)


def publish(
    producer: Producer,
    topic: str,
    value: dict[str, Any],
    key: str | None = None,
) -> None:
    """
    Synchronously-ish publish one JSON message.
    Uses poll(0) to drain the delivery queue without blocking;
    call producer.flush() at the end of a batch for a hard guarantee.
    """
    def _on_delivery(err, msg):
        if err:
            log.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            log.debug("Delivered → %s [%d]", msg.topic(), msg.partition())

    producer.produce(
        topic=topic,
        key=key.encode() if key else None,
        value=json.dumps(value).encode(),
        callback=_on_delivery,
    )
    producer.poll(0)  # trigger callbacks without blocking


# ── Consumer ──────────────────────────────────────────────────────────────────

def make_consumer(settings: Settings, group_id: str, topics: list[str]) -> Consumer:
    cfg = settings.kafka_ssl_config() | {
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 3600000,  # 30 min — handles large groups
        # Disable auto-commit — we commit manually after successful processing
        "enable.auto.commit": False,
    }
    consumer = Consumer(cfg)
    consumer.subscribe(topics)
    return consumer


# ── Admin (topic creation) ────────────────────────────────────────────────────

def ensure_topics(settings: Settings, topics: list[str], num_partitions: int = 6) -> None:
    """
    Idempotently create topics if they don't already exist.
    Safe to call on every startup.
    num_partitions=6 gives room for up to 6 parallel workers per topic.
    """
    admin = AdminClient(settings.kafka_ssl_config())
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=1)
        for t in topics
        if t not in existing
    ]
    if not to_create:
        log.info("All topics already exist.")
        return

    results = admin.create_topics(to_create)
    for topic, future in results.items():
        try:
            future.result()
            log.info("Created topic: %s", topic)
        except Exception as exc:
            # TopicExistsException is fine — race condition between workers
            if "TopicExistsException" not in str(type(exc)):
                raise
