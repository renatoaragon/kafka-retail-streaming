"""Tests for the Iceberg sink wiring (no Spark/Iceberg runtime needed)."""

from retail_stream.iceberg import (
    ICEBERG_EXTENSIONS,
    ICEBERG_PACKAGE,
    configure_iceberg_writer,
    iceberg_configs,
)


def test_iceberg_configs_wire_a_hadoop_catalog():
    cfg = iceberg_configs(catalog="local", warehouse="/tmp/wh")
    assert cfg["spark.jars.packages"] == ICEBERG_PACKAGE
    assert cfg["spark.sql.extensions"] == ICEBERG_EXTENSIONS
    assert cfg["spark.sql.catalog.local"] == "org.apache.iceberg.spark.SparkCatalog"
    assert cfg["spark.sql.catalog.local.type"] == "hadoop"
    assert cfg["spark.sql.catalog.local.warehouse"] == "/tmp/wh"


def test_iceberg_configs_respect_catalog_name():
    cfg = iceberg_configs(catalog="prod")
    assert "spark.sql.catalog.prod" in cfg
    assert "spark.sql.catalog.local" not in cfg


class FakeWriter:
    """Records the fluent calls made by configure_iceberg_writer."""

    def __init__(self):
        self.calls = []

    def format(self, fmt):
        self.calls.append(("format", fmt))
        return self

    def outputMode(self, mode):
        self.calls.append(("outputMode", mode))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self


def test_configure_writer_sets_format_mode_and_checkpoint():
    w = FakeWriter()
    result = configure_iceberg_writer(w, checkpoint="/tmp/ckpt", mode="complete")

    assert result is w
    assert ("format", "iceberg") in w.calls
    assert ("outputMode", "complete") in w.calls
    assert ("option", "checkpointLocation", "/tmp/ckpt") in w.calls


def test_configure_writer_defaults_to_append():
    w = FakeWriter()
    configure_iceberg_writer(w, checkpoint="/tmp/ckpt")
    assert ("outputMode", "append") in w.calls
