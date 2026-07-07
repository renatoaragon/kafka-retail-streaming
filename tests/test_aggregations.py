"""Tests for windowed aggregations, on static DataFrames (no stream needed).

`window` and `withWatermark` are valid on batch DataFrames, so the bucketing and
sums are verified deterministically here.
"""

from datetime import datetime

from pyspark.sql import functions as F

from retail_stream.aggregations import revenue_sliding, revenue_tumbling, sale_revenue

# event_type, category, quantity, unit_price, event_time
COLS = ["event_type", "category", "quantity", "unit_price", "event_time"]


def _events(spark, rows):
    df = spark.createDataFrame(rows, COLS)
    return df.withColumn("event_time", F.col("event_time").cast("timestamp"))


def test_sale_revenue_filters_and_computes(spark):
    rows = [
        ("sale", "books", 2, 10.0, datetime(2025, 1, 1, 10, 0, 0)),
        ("stock", "books", 5, 0.0, datetime(2025, 1, 1, 10, 0, 5)),  # dropped
    ]
    result = sale_revenue(_events(spark, rows)).collect()
    assert len(result) == 1
    assert result[0]["revenue"] == 20.0


def test_tumbling_buckets_by_minute(spark):
    rows = [
        # two sales in the 10:00 minute, one in 10:01 — same category
        ("sale", "books", 2, 10.0, datetime(2025, 1, 1, 10, 0, 10)),  # 20
        ("sale", "books", 1, 30.0, datetime(2025, 1, 1, 10, 0, 50)),  # 30
        ("sale", "books", 1, 15.0, datetime(2025, 1, 1, 10, 1, 5)),   # 15
    ]
    out = {
        (str(r["window_start"]), r["category"]): r
        for r in revenue_tumbling(_events(spark, rows), window="1 minute").collect()
    }
    first = out[("2025-01-01 10:00:00", "books")]
    assert first["revenue"] == 50.0
    assert first["orders"] == 2
    second = out[("2025-01-01 10:01:00", "books")]
    assert second["revenue"] == 15.0
    assert second["orders"] == 1


def test_sliding_windows_overlap(spark):
    # One sale should appear in multiple overlapping 5-min windows (slide 1 min).
    rows = [("sale", "toys", 1, 100.0, datetime(2025, 1, 1, 10, 2, 30))]
    windows = revenue_sliding(
        _events(spark, rows), window="5 minutes", slide="1 minute"
    ).collect()

    # A 5-min window sliding every 1 min covers a single instant 5 times.
    assert len(windows) == 5
    assert all(w["revenue"] == 100.0 for w in windows)
    # Every window actually contains the event instant.
    event_instant = datetime(2025, 1, 1, 10, 2, 30)
    for w in windows:
        assert w["window_start"] <= event_instant < w["window_end"]
