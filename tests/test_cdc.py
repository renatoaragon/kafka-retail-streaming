"""Tests for CDC consumption: envelope parsing, flattening, compaction, MERGE.

All on static DataFrames — the payloads are byte-for-byte what Debezium
produces with schemas.enable=false and decimal.handling.mode=double.
"""

import json

from pyspark.sql import functions as F

from retail_stream.cdc import (
    OP_DELETE,
    flatten_changes,
    latest_change_per_key,
    merge_into_sql,
    parse_cdc_events,
)


def _row(sku, price, stock=10, category="books", ts="2025-01-01T10:00:00Z"):
    return {
        "sku": sku, "category": category, "unit_price": price,
        "stock": stock, "updated_at": ts,
    }


def _envelope(op, before=None, after=None, ts_ms=0):
    return json.dumps({"before": before, "after": after, "op": op, "ts_ms": ts_ms})


def _raw(spark, payloads):
    # value is binary on a real topic; None models a log-compaction tombstone.
    rows = [(bytearray(p.encode("utf-8")) if p is not None else None,) for p in payloads]
    return spark.createDataFrame(rows, "value binary")


def test_parse_decodes_the_debezium_envelope(spark):
    payloads = [
        _envelope("c", after=_row("SKU1", 12.5), ts_ms=100),
        _envelope("u", before=_row("SKU1", 12.5), after=_row("SKU1", 13.9), ts_ms=200),
        _envelope("d", before=_row("SKU1", 13.9), ts_ms=300),
        _envelope("r", after=_row("SKU2", 8.75), ts_ms=50),  # snapshot read
    ]
    events = parse_cdc_events(_raw(spark, payloads)).orderBy("ts_ms").collect()

    assert [e["op"] for e in events] == ["r", "c", "u", "d"]
    update = events[2]
    assert update["before"]["unit_price"] == 12.5
    assert update["after"]["unit_price"] == 13.9


def test_parse_drops_tombstones_and_garbage(spark):
    payloads = [
        _envelope("c", after=_row("SKU1", 12.5)),
        None,             # Kafka log-compaction tombstone (null value)
        "not even json",  # garbage
    ]
    events = parse_cdc_events(_raw(spark, payloads))
    assert events.count() == 1


def test_flatten_takes_key_from_before_on_delete(spark):
    payloads = [
        _envelope("u", before=_row("SKU1", 12.5), after=_row("SKU1", 13.9), ts_ms=1),
        _envelope("d", before=_row("SKU2", 8.75), ts_ms=2),
    ]
    changes = {
        r["sku"]: r
        for r in flatten_changes(parse_cdc_events(_raw(spark, payloads))).collect()
    }

    assert changes["SKU1"]["unit_price"] == 13.9  # update carries after-values
    delete = changes["SKU2"]
    assert delete["op"] == OP_DELETE
    assert delete["unit_price"] is None  # only the key matters for a delete


def test_compaction_keeps_only_the_final_change_per_key(spark):
    payloads = [
        _envelope("c", after=_row("SKU1", 10.0), ts_ms=100),
        _envelope("u", after=_row("SKU1", 11.0), ts_ms=200),
        _envelope("u", after=_row("SKU1", 12.0), ts_ms=300),  # final for SKU1
        _envelope("c", after=_row("SKU2", 8.0), ts_ms=150),   # untouched
    ]
    changes = flatten_changes(parse_cdc_events(_raw(spark, payloads)))
    compacted = {r["sku"]: r for r in latest_change_per_key(changes).collect()}

    assert len(compacted) == 2
    assert compacted["SKU1"]["unit_price"] == 12.0
    assert compacted["SKU2"]["unit_price"] == 8.0


def test_compaction_lets_a_trailing_delete_win(spark):
    payloads = [
        _envelope("u", after=_row("SKU1", 11.0), ts_ms=100),
        _envelope("d", before=_row("SKU1", 11.0), ts_ms=200),
    ]
    changes = flatten_changes(parse_cdc_events(_raw(spark, payloads)))
    compacted = latest_change_per_key(changes).collect()

    assert len(compacted) == 1
    assert compacted[0]["op"] == OP_DELETE  # the delete is the final word


def test_merge_sql_handles_all_three_outcomes():
    sql = merge_into_sql("local.db.products")

    assert "MERGE INTO local.db.products t" in sql
    assert "WHEN MATCHED AND s.op = 'd' THEN DELETE" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    # A delete for a row we never saw must NOT insert a null husk.
    assert "WHEN NOT MATCHED AND s.op != 'd' THEN INSERT" in sql
