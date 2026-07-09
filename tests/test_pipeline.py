"""End-to-end pipeline test on static data: produce -> decode -> route -> aggregate.

Every prior test exercises one stage against hand-written payloads. This one runs
the whole chain with each stage's real code: events from the seeded generator go
through ``publish`` (captured by a fake producer, byte-for-byte what would land on
the topic), are decoded by the consumer, routed by the valid/late splits, and
aggregated into windowed revenue. Two properties only an end-to-end run can catch:

- **Contract** — the producer's wire format and the consumer's ``EVENT_SCHEMA``
  are tested against each other; drift on either side fails here first.
- **Conservation** — every record entering the pipeline comes out in exactly one
  place (the events path, or the DLQ with a reason); nothing is silently dropped.

Static DataFrames stand in for the stream, as in the other tests; the streaming
source and sink themselves are exercised against the running stack, not in CI.
"""

from datetime import datetime

import pytest

from retail_stream.aggregations import revenue_tumbling
from retail_stream.consumer import decode_with_raw
from retail_stream.dlq import (
    REASON_LATE,
    REASON_MALFORMED,
    as_dlq,
    split_on_time_late,
    split_valid_invalid,
)
from retail_stream.events import GeneratorConfig, generate_events
from retail_stream.producer import publish

N_EVENTS = 40
SEED = 7

# Generator events start at 2025-01-01T00:00:00Z, 5s apart -> the first six fall
# before this cutoff and must be routed as late.
CUTOFF = datetime(2025, 1, 1, 0, 0, 30)

# Records a real topic could also hold: garbage, an unknown type, a broken ts.
POISON = [
    b"not even json",
    b'{"event_type":"mystery","event_id":"m-1","ts":"2025-01-01T00:01:00Z"}',
    b'{"event_type":"sale","event_id":"bad-ts","ts":"around noonish",'
    b'"category":"books","quantity":1,"unit_price":10.0}',
]


class _CapturingProducer:
    """The ProducerLike slice, capturing records instead of talking to a broker."""

    def __init__(self):
        self.records = []

    def produce(self, topic, key, value):
        self.records.append((topic, key, value))

    def flush(self, timeout=None):
        return 0


def _source_events():
    return list(generate_events(N_EVENTS, GeneratorConfig(seed=SEED)))


def _ts(event):
    return datetime.strptime(event["ts"], "%Y-%m-%dT%H:%M:%SZ")


def _topic_payloads(events):
    """Run events through the real produce path; return the values as Kafka holds them."""
    fake = _CapturingProducer()
    publish(fake, "retail.events", events)
    return [value for _, _, value in fake.records]


def _run_pipeline(spark, payloads):
    """The full static-DataFrame pipeline, mirroring the streaming wiring."""
    raw = spark.createDataFrame([(bytearray(p),) for p in payloads], "value binary")
    decoded = decode_with_raw(raw)
    valid, invalid = split_valid_invalid(decoded)
    on_time, late = split_on_time_late(valid, CUTOFF)
    dlq = as_dlq(invalid, REASON_MALFORMED).unionByName(as_dlq(late, REASON_LATE))
    return {
        "valid": valid,
        "invalid": invalid,
        "on_time": on_time,
        "late": late,
        "dlq": dlq,
        "revenue": revenue_tumbling(on_time),
    }


def test_producer_wire_format_matches_consumer_schema(spark):
    events = _source_events()
    p = _run_pipeline(spark, _topic_payloads(events))

    # Nothing the producer emits is rejected by the consumer's schema.
    assert p["invalid"].count() == 0
    assert p["valid"].count() == len(events)

    # And the fields survive the round trip, for both event types.
    by_id = {r["event_id"]: r for r in p["valid"].collect()}
    sale = next(e for e in events if e["event_type"] == "sale")
    row = by_id[sale["event_id"]]
    assert (row["order_id"], row["category"]) == (sale["order_id"], sale["category"])
    assert (row["quantity"], row["unit_price"]) == (sale["quantity"], sale["unit_price"])
    stock = next(e for e in events if e["event_type"] == "stock")
    row = by_id[stock["event_id"]]
    assert (row["sku"], row["warehouse"]) == (stock["sku"], stock["warehouse"])
    assert row["quantity_delta"] == stock["quantity_delta"]


def test_every_record_lands_exactly_once(spark):
    payloads = _topic_payloads(_source_events()) + POISON
    p = _run_pipeline(spark, payloads)

    n_valid, n_invalid = p["valid"].count(), p["invalid"].count()
    assert n_valid + n_invalid == len(payloads)  # decode splits, never drops
    assert p["on_time"].count() + p["late"].count() == n_valid  # so does the cutoff
    assert p["on_time"].count() + p["dlq"].count() == len(payloads)  # out + DLQ == in


def test_dlq_accounts_for_every_reject_with_reason_and_payload(spark):
    events = _source_events()
    p = _run_pipeline(spark, _topic_payloads(events) + POISON)

    n_late_expected = sum(1 for e in events if _ts(e) < CUTOFF)
    assert n_late_expected > 0  # guard: the fixture must exercise the late path

    rows = p["dlq"].collect()
    by_reason = {}
    for r in rows:
        by_reason[r["dlq_reason"]] = by_reason.get(r["dlq_reason"], 0) + 1
    assert by_reason == {
        REASON_MALFORMED: len(POISON),
        REASON_LATE: n_late_expected,
    }
    assert all(r["raw_json"] for r in rows)  # original payload always preserved


def test_windowed_revenue_matches_independent_computation(spark):
    events = _source_events()
    p = _run_pipeline(spark, _topic_payloads(events))

    # Recompute the expected aggregate in plain Python, from the source dicts.
    expected = {}
    for e in events:
        if e["event_type"] != "sale" or _ts(e) < CUTOFF:
            continue
        key = (_ts(e).replace(second=0), e["category"])  # 1-minute tumbling bucket
        revenue, orders = expected.get(key, (0.0, 0))
        expected[key] = (revenue + round(e["quantity"] * e["unit_price"], 2), orders + 1)
    assert expected  # guard: the fixture must produce on-time sales

    actual = {
        (r["window_start"], r["category"]): (r["revenue"], r["orders"])
        for r in p["revenue"].collect()
    }
    assert set(actual) == set(expected)
    for key, (revenue, orders) in expected.items():
        assert actual[key][0] == pytest.approx(round(revenue, 2), abs=1e-9)
        assert actual[key][1] == orders
