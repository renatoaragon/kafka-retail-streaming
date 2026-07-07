"""Spark Structured Streaming consumer for the ``retail.events`` topic.

This stage does the basic read: subscribe to Kafka, decode the JSON payload into a
typed DataFrame, and print it to the console. The decoding step (``parse_events``)
is a plain ``DataFrame -> DataFrame`` transformation, so it is unit-tested on a
static DataFrame without a running broker; only the streaming source and sink need
Kafka.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

TOPIC = "retail.events"
DEFAULT_BOOTSTRAP = "localhost:9092"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# One schema spans both event types; fields absent from a given type stay null.
EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("ts", StringType()),
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("sku", StringType()),
        StructField("warehouse", StringType()),
        StructField("quantity_delta", IntegerType()),
    ]
)


def build_spark(app_name: str = "retail-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Decode Kafka records (binary ``value`` JSON) into typed event columns.

    Works on both streaming and static DataFrames — it is a pure transformation,
    which is what lets it be tested without Kafka.
    """
    return (
        raw.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("ts"))
    )


def read_kafka_stream(
    spark: SparkSession, bootstrap_servers: str, topic: str = TOPIC
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def main() -> None:  # pragma: no cover - needs a live broker
    parser = argparse.ArgumentParser(description="Consume retail events from Kafka.")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    events = parse_events(read_kafka_stream(spark, args.bootstrap, args.topic))

    query = (
        events.writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
