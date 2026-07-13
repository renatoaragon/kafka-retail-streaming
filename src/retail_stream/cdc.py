"""Consume Debezium change events for ``products`` into an Iceberg table.

This is where the promise in ADR 0001 comes due: the plain streaming sink is
append-shaped, but CDC is **update-shaped** — a change event supersedes the
row's previous state, and a delete removes it. The right tool, as the ADR's
alternatives section anticipated, is ``foreachBatch`` + ``MERGE INTO``: each
micro-batch is compacted to the latest change per key and merged atomically
into the target table.

Everything except the live stream wiring is a pure transformation or a string
builder, tested on static data without a broker or the Iceberg runtime.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

CDC_TOPIC = "retail.cdc.public.products"

OP_CREATE, OP_UPDATE, OP_DELETE, OP_READ = "c", "u", "d", "r"

# One row of the products table as Debezium serializes it with
# schemas.enable=false and decimal.handling.mode=double (numeric would
# otherwise arrive as base64 bytes; timestamptz arrives as an ISO string).
ROW_SCHEMA = StructType(
    [
        StructField("sku", StringType()),
        StructField("category", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("stock", IntegerType()),
        StructField("updated_at", StringType()),
    ]
)

# The Debezium envelope: before/after row images, the operation and its time.
ENVELOPE_SCHEMA = StructType(
    [
        StructField("before", ROW_SCHEMA),
        StructField("after", ROW_SCHEMA),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)


def parse_cdc_events(raw: DataFrame) -> DataFrame:
    """Decode Kafka records into typed Debezium envelopes.

    Kafka log-compaction tombstones (null value, produced because the
    connector sets ``tombstones.on.delete``) and unparseable records decode to
    a null ``op`` and are dropped — the delete itself arrives as a regular
    ``op='d'`` event, so nothing meaningful is lost.
    """
    return (
        raw.select(F.col("value").cast("string").alias("raw_json"))
        .select(F.from_json("raw_json", ENVELOPE_SCHEMA).alias("e"))
        .select("e.before", "e.after", "e.op", "e.ts_ms")
        .filter(F.col("op").isNotNull())
    )


def flatten_changes(events: DataFrame) -> DataFrame:
    """One row per change: key, latest known column values, op and event time.

    For creates/updates/snapshot reads the values come from ``after``; for
    deletes ``after`` is null, so the key comes from ``before`` and the value
    columns stay null (MERGE only needs the key to delete).
    """
    return events.select(
        F.coalesce(F.col("after.sku"), F.col("before.sku")).alias("sku"),
        F.col("after.category").alias("category"),
        F.col("after.unit_price").alias("unit_price"),
        F.col("after.stock").alias("stock"),
        F.col("after.updated_at").alias("updated_at"),
        "op",
        "ts_ms",
    )


def latest_change_per_key(changes: DataFrame) -> DataFrame:
    """Batch compaction: keep only each key's final change within the batch.

    A key touched five times in one micro-batch has one true final state; the
    intermediate versions would only churn the MERGE (and a stale one applied
    last would corrupt the table). ``ts_ms`` orders the changes.
    """
    latest = Window.partitionBy("sku").orderBy(
        F.col("ts_ms").desc()
    )
    return (
        changes.withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def merge_into_sql(target: str, source_view: str = "cdc_updates") -> str:
    """The MERGE applying one compacted batch to the Iceberg table."""
    return f"""
        MERGE INTO {target} t
        USING {source_view} s
        ON t.sku = s.sku
        WHEN MATCHED AND s.op = '{OP_DELETE}' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            t.category = s.category,
            t.unit_price = s.unit_price,
            t.stock = s.stock,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED AND s.op != '{OP_DELETE}' THEN INSERT
            (sku, category, unit_price, stock, updated_at)
            VALUES (s.sku, s.category, s.unit_price, s.stock, s.updated_at)
    """


def apply_cdc_batch(  # pragma: no cover - MERGE needs the Iceberg runtime
    batch_df: DataFrame, batch_id: int, target: str
) -> None:
    """foreachBatch hook: compact the batch and merge it into the target."""
    compacted = latest_change_per_key(batch_df)
    compacted.createOrReplaceTempView("cdc_updates")
    batch_df.sparkSession.sql(merge_into_sql(target))


def stream_cdc_to_iceberg(  # pragma: no cover - live wiring
    spark, bootstrap_servers: str, target: str, checkpoint: str
):
    """Read the CDC topic and continuously merge changes into Iceberg."""
    from retail_stream.consumer import read_kafka_stream

    raw = read_kafka_stream(spark, bootstrap_servers, CDC_TOPIC)
    changes = flatten_changes(parse_cdc_events(raw))
    return (
        changes.writeStream.foreachBatch(
            lambda df, bid: apply_cdc_batch(df, bid, target)
        )
        .option("checkpointLocation", checkpoint)
        .start()
    )
