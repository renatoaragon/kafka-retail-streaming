"""Sanity checks on the CDC wiring files (no containers needed).

The compose graph itself is validated in CI with ``docker compose config``;
these tests pin the parts of the CDC setup that the compose schema cannot see —
the connector registration document and the init SQL — so a typo'd key or a
dropped setting fails in the suite instead of at first ``curl`` against a live
stack.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR_PATH = ROOT / "cdc" / "register-postgres.json"
INIT_SQL_PATH = ROOT / "cdc" / "initdb" / "01-products.sql"


def _connector():
    return json.loads(CONNECTOR_PATH.read_text(encoding="utf-8"))


def test_connector_document_has_registration_shape():
    doc = _connector()
    # The Connect REST API expects exactly this envelope: a name + a config map.
    assert set(doc) == {"name", "config"}
    assert doc["name"] == "retail-products-cdc"
    assert isinstance(doc["config"], dict)


def test_connector_uses_native_pgoutput_against_the_compose_postgres():
    cfg = _connector()["config"]
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    # pgoutput is Postgres' built-in logical decoding plugin: nothing extra to
    # install in the database image.
    assert cfg["plugin.name"] == "pgoutput"
    # Must match the compose service (hostname/credentials/db).
    assert cfg["database.hostname"] == "postgres"
    assert cfg["database.dbname"] == "retail"
    assert cfg["database.user"] == "retail"


def test_connector_targets_products_with_a_stable_topic_prefix():
    cfg = _connector()["config"]
    assert cfg["table.include.list"] == "public.products"
    # Change events will land on retail.cdc.public.products.
    assert cfg["topic.prefix"] == "retail.cdc"
    assert cfg["snapshot.mode"] == "initial"


def test_connector_declares_topic_creation_since_broker_autocreate_is_off():
    # The broker runs with auto.create.topics.enable=false (deliberate, PR #1),
    # so the change-event topics must be created explicitly by Connect. Without
    # these settings the connector would start and then hang producing nowhere.
    cfg = _connector()["config"]
    assert cfg["topic.creation.default.replication.factor"] == "1"
    assert int(cfg["topic.creation.default.partitions"]) >= 1


def test_init_sql_creates_seeds_and_sets_replica_identity():
    sql = INIT_SQL_PATH.read_text(encoding="utf-8")
    assert "CREATE TABLE products" in sql
    assert "INSERT INTO products" in sql
    # FULL before-images: update/delete events carry the old row, not just its key.
    assert "REPLICA IDENTITY FULL" in sql
    # The table the connector includes must be the one the SQL creates.
    table = _connector()["config"]["table.include.list"].split(".", 1)[1]
    assert f"CREATE TABLE {table}" in sql
