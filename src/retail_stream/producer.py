"""Publish synthetic retail events to Kafka as JSON.

The serialization and send loop (``publish``) are decoupled from the Kafka client
so they can be unit tested with a fake producer — no broker required. Only
``build_producer`` touches ``confluent_kafka``, and it does so with a lazy import
so the rest of the module (and its tests) load without the native library.
"""

from __future__ import annotations

import argparse
import json
from typing import Iterable, Protocol

from retail_stream.events import GeneratorConfig, event_key, generate_events

DEFAULT_TOPIC = "retail.events"
DEFAULT_BOOTSTRAP = "localhost:9092"


class ProducerLike(Protocol):
    """The slice of the confluent_kafka Producer API that ``publish`` needs."""

    def produce(self, topic: str, key: bytes, value: bytes) -> None: ...

    def flush(self, timeout: float | None = None) -> int: ...


def serialize(event: dict) -> bytes:
    """Encode an event as compact, sorted-key JSON bytes (stable on the wire)."""
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")


def publish(producer: ProducerLike, topic: str, events: Iterable[dict]) -> int:
    """Produce each event keyed by its partition key; return the count sent."""
    sent = 0
    for event in events:
        producer.produce(
            topic=topic,
            key=event_key(event).encode("utf-8"),
            value=serialize(event),
        )
        sent += 1
    producer.flush()
    return sent


def build_producer(bootstrap_servers: str):  # pragma: no cover - needs native lib
    """Create a confluent_kafka Producer (imported lazily so tests stay lib-free)."""
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": bootstrap_servers})


def main() -> None:  # pragma: no cover - CLI wiring
    parser = argparse.ArgumentParser(description="Produce synthetic retail events.")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)
    events = generate_events(args.count, GeneratorConfig(seed=args.seed))
    sent = publish(producer, args.topic, events)
    print(f"Produced {sent} events to {args.topic} on {args.bootstrap}")


if __name__ == "__main__":
    main()
