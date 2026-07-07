"""Iceberg sink for the streaming pipeline, with checkpointing.

Writes a streaming DataFrame to an Apache Iceberg table. The Spark configuration
and the writer wiring are factored into small pure helpers so they can be unit
tested with a fake writer; actually running the write needs the Iceberg runtime
jar and a live stream, so those parts are exercised against the running stack, not
in CI.

Checkpointing is the crux: the ``checkpointLocation`` holds the Kafka offsets and
aggregation state for the query. On restart Spark resumes from it, which — together
with Iceberg's atomic commits — is what gives the pipeline its recovery guarantees
(explored further in the exactly-once ADR).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

DEFAULT_CATALOG = "local"
DEFAULT_WAREHOUSE = "warehouse"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def iceberg_configs(
    catalog: str = DEFAULT_CATALOG, warehouse: str = DEFAULT_WAREHOUSE
) -> dict:
    """Spark configs for a local Hadoop-catalog Iceberg setup."""
    return {
        "spark.jars.packages": ICEBERG_PACKAGE,
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "hadoop",
        f"spark.sql.catalog.{catalog}.warehouse": warehouse,
    }


def build_spark_iceberg(  # pragma: no cover - needs the Iceberg runtime jar
    app_name: str = "retail-stream-iceberg",
    catalog: str = DEFAULT_CATALOG,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in iceberg_configs(catalog, warehouse).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def configure_iceberg_writer(writer, checkpoint: str, mode: str = "append"):
    """Wire a streaming writer to Iceberg with checkpointing (pure, testable).

    Kept separate from ``.toTable(...)`` (which starts the query) so the wiring can
    be asserted with a fake writer.
    """
    return (
        writer.format("iceberg")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
    )


def write_stream_to_iceberg(  # pragma: no cover - starts a live query
    df: DataFrame, table: str, checkpoint: str, mode: str = "append"
):
    return configure_iceberg_writer(df.writeStream, checkpoint, mode).toTable(table)
