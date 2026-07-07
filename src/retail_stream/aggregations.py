"""Windowed aggregations over the parsed event stream.

Two windowing strategies on **event time** (the ``event_time`` column derived by
``parse_events``), each guarded by a **watermark** so Spark can bound how long it
waits for late data and drop state for windows that can no longer receive updates:

- **Tumbling** — fixed, non-overlapping windows (e.g. revenue per 1-minute bucket).
  Every event falls in exactly one window.
- **Sliding** — fixed-length windows that advance by a smaller slide (e.g. a
  5-minute window every 1 minute), so windows overlap and an event contributes to
  several. Useful for moving trends.

All functions are pure ``DataFrame -> DataFrame`` transforms. ``withWatermark`` and
``window`` are valid on batch DataFrames too, so the aggregation logic is unit
tested on static data without a running stream.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SALE = "sale"


def sale_revenue(events: DataFrame) -> DataFrame:
    """Keep sale events and attach ``revenue = quantity * unit_price``."""
    return events.filter(F.col("event_type") == SALE).withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )


def _finalize(windowed: DataFrame) -> DataFrame:
    """Flatten the struct ``window`` column into explicit start/end columns."""
    return windowed.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "category",
        F.round(F.col("revenue"), 2).alias("revenue"),
        F.col("orders"),
    ).orderBy("window_start", "category")


def revenue_tumbling(
    events: DataFrame,
    window: str = "1 minute",
    watermark: str = "2 minutes",
) -> DataFrame:
    """Revenue and order count per category in non-overlapping ``window`` buckets."""
    return _finalize(
        sale_revenue(events)
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window), "category")
        .agg(
            F.sum("revenue").alias("revenue"),
            F.count("*").alias("orders"),
        )
    )


def revenue_sliding(
    events: DataFrame,
    window: str = "5 minutes",
    slide: str = "1 minute",
    watermark: str = "2 minutes",
) -> DataFrame:
    """Revenue and order count over overlapping windows advancing by ``slide``."""
    return _finalize(
        sale_revenue(events)
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window, slide), "category")
        .agg(
            F.sum("revenue").alias("revenue"),
            F.count("*").alias("orders"),
        )
    )
