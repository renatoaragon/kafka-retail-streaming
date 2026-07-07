"""Tests for late-data routing and the dead-letter queue, on static DataFrames."""

from datetime import datetime

from pyspark.sql import functions as F

from retail_stream.consumer import decode_with_raw
from retail_stream.dlq import (
    REASON_LATE,
    REASON_MALFORMED,
    as_dlq,
    split_on_time_late,
    split_valid_invalid,
)


def _raw(spark, payloads):
    # payloads: list of JSON strings (or garbage) as they'd arrive on Kafka.
    return spark.createDataFrame([(p,) for p in payloads], ["value"])


def test_split_valid_invalid_routes_bad_records(spark):
    payloads = [
        '{"event_type":"sale","event_id":"sale-1","ts":"2025-01-01T10:00:00Z",'
        '"category":"books","quantity":1,"unit_price":10.0}',  # valid
        '{"event_type":"stock","event_id":"stock-1","ts":"2025-01-01T10:00:05Z",'
        '"category":"home","warehouse":"PT-LIS","quantity_delta":5}',  # valid
        "not even json",  # malformed -> null fields
        '{"event_type":"mystery","event_id":"x","ts":"2025-01-01T10:00:10Z"}',  # unknown type
    ]
    valid, invalid = split_valid_invalid(decode_with_raw(_raw(spark, payloads)))

    assert valid.count() == 2
    assert {r["event_type"] for r in valid.collect()} == {"sale", "stock"}
    assert invalid.count() == 2


def test_split_on_time_late_partitions_by_watermark(spark):
    payloads = [
        '{"event_type":"sale","event_id":"a","ts":"2025-01-01T10:00:00Z",'
        '"category":"books","quantity":1,"unit_price":5.0}',  # late
        '{"event_type":"sale","event_id":"b","ts":"2025-01-01T10:05:00Z",'
        '"category":"books","quantity":1,"unit_price":5.0}',  # on time
    ]
    events = decode_with_raw(_raw(spark, payloads))
    cutoff = datetime(2025, 1, 1, 10, 2, 0)

    on_time, late = split_on_time_late(events, cutoff)

    assert [r["event_id"] for r in on_time.collect()] == ["b"]
    assert [r["event_id"] for r in late.collect()] == ["a"]


def test_as_dlq_carries_payload_and_reason(spark):
    payloads = ["totally broken"]
    _, invalid = split_valid_invalid(decode_with_raw(_raw(spark, payloads)))

    dlq = as_dlq(invalid, REASON_MALFORMED).collect()
    assert len(dlq) == 1
    assert dlq[0]["dlq_reason"] == REASON_MALFORMED
    assert dlq[0]["raw_json"] == "totally broken"
    assert dlq[0]["event_id"] == "unknown"  # null id coalesced


def test_reason_constants_distinct():
    assert REASON_MALFORMED != REASON_LATE
