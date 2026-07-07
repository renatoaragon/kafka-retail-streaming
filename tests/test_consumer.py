"""Tests for the streaming decode step.

``parse_events`` is exercised on a static DataFrame that mimics what Kafka hands
Spark (a binary ``value`` column holding JSON), so no broker is needed.
"""

from retail_stream.consumer import parse_events
from retail_stream.events import GeneratorConfig, generate_events
from retail_stream.producer import serialize


def _raw_from_events(spark, events):
    # Kafka delivers `value` as bytes; simulate that so the cast("string") path runs.
    rows = [(serialize(e),) for e in events]
    return spark.createDataFrame(rows, ["value"])


def test_parse_decodes_sale_and_stock(spark):
    events = list(generate_events(200, GeneratorConfig(seed=4)))
    parsed = {r["event_id"]: r for r in parse_events(_raw_from_events(spark, events)).collect()}

    assert len(parsed) == len(events)
    by_id = {e["event_id"]: e for e in events}
    for eid, row in parsed.items():
        src = by_id[eid]
        assert row["event_type"] == src["event_type"]
        assert row["category"] == src["category"]
        if src["event_type"] == "sale":
            assert row["quantity"] == src["quantity"]
            assert row["order_id"] == src["order_id"]
            assert row["sku"] is None  # field absent from sales stays null
        else:
            assert row["quantity_delta"] == src["quantity_delta"]
            assert row["sku"] == src["sku"]
            assert row["order_id"] is None


def test_parse_sets_event_time_timestamp(spark):
    events = list(generate_events(10, GeneratorConfig(seed=1)))
    parsed = parse_events(_raw_from_events(spark, events))

    assert dict(parsed.dtypes)["event_time"] == "timestamp"
    assert parsed.filter("event_time is null").count() == 0


def test_parse_schema_has_expected_columns(spark):
    parsed = parse_events(_raw_from_events(spark, list(generate_events(1))))
    for col in ("event_type", "event_id", "ts", "category", "event_time"):
        assert col in parsed.columns
