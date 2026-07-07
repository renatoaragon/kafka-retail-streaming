"""Synthetic retail event model and a deterministic generator.

Two event types flow through the platform, in the same retail domain as
``spark-retail-etl``:

- ``sale``  — an order line was sold (drives revenue/throughput aggregations).
- ``stock`` — a warehouse stock level changed (positive = restock, negative = pick).

The generator is seeded, so a given seed always yields the same stream — which
makes the producer reproducible and the tests deterministic. This module has no
Kafka dependency on purpose: it is pure data, trivial to unit test.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

CATEGORIES = ("books", "home", "toys", "beauty", "sports")
WAREHOUSES = ("PT-LIS", "ES-MAD", "FR-PAR")

SALE = "sale"
STOCK = "stock"

# A fixed epoch so timestamps are deterministic without reading the wall clock.
_EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    sale_ratio: float = 0.7  # share of events that are sales vs stock moves
    step_seconds: int = 5  # spacing between consecutive events


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sale(rng: random.Random, seq: int, ts: datetime) -> dict:
    return {
        "event_type": SALE,
        "event_id": f"sale-{seq:08d}",
        "ts": _iso(ts),
        "order_id": f"O{rng.randint(1, 10_000):06d}",
        "customer_id": f"C{rng.randint(1, 2_000):05d}",
        "category": rng.choice(CATEGORIES),
        "quantity": rng.randint(1, 5),
        "unit_price": round(rng.uniform(3.0, 120.0), 2),
    }


def _stock(rng: random.Random, seq: int, ts: datetime) -> dict:
    return {
        "event_type": STOCK,
        "event_id": f"stock-{seq:08d}",
        "ts": _iso(ts),
        "sku": f"SKU{rng.randint(1, 500):04d}",
        "category": rng.choice(CATEGORIES),
        "warehouse": rng.choice(WAREHOUSES),
        "quantity_delta": rng.choice([-3, -2, -1, 1, 5, 10, 20]),
    }


def generate_events(
    count: int, config: GeneratorConfig | None = None
) -> Iterator[dict]:
    """Yield ``count`` retail events as plain dicts, deterministic per seed."""
    if count < 0:
        raise ValueError("count must be non-negative")
    cfg = config or GeneratorConfig()
    rng = random.Random(cfg.seed)
    ts = _EPOCH
    for seq in range(count):
        if rng.random() < cfg.sale_ratio:
            yield _sale(rng, seq, ts)
        else:
            yield _stock(rng, seq, ts)
        ts += timedelta(seconds=cfg.step_seconds)


def event_key(event: dict) -> str:
    """Partition key: order for sales, sku for stock — keeps a key's events ordered."""
    return event.get("order_id") or event.get("sku") or event["event_id"]
