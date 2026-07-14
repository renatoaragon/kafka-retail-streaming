# kafka-retail-streaming

![License](https://img.shields.io/badge/license-MIT-green)

A real-time streaming companion to
[spark-retail-etl](https://github.com/renatoaragon/spark-retail-etl): the same
retail domain, but events flowing continuously through **Kafka** and processed with
**Spark Structured Streaming** instead of a nightly batch.

The project is built up in small, reviewable steps. Each stage lands as its own
pull request with a short design note, so the history reads as a series of
decisions rather than a single drop.

> Runs locally with Docker and synthetic data. No real or personal data is used.

## Architecture

```
                 ┌──────────────┐
   seeded        │   producer   │  synthetic sale / stock events (JSON)
   generator ───▶│ retail_stream│  keyed by order_id / sku
                 └──────┬───────┘
                        ▼  topic: retail.events
                 ┌──────────────┐        ┌──────────────────┐
                 │    Kafka     │◀──────▶│ Schema Registry  │
                 │   (KRaft)    │        │  (Avro, planned) │
                 └──────┬───────┘        └──────────────────┘
                        ▼
                 ┌──────────────┐   windowed aggregations (tumbling + sliding)
                 │    Spark     │   watermarks · late data → dead-letter queue
                 │  Structured  │
                 │  Streaming   │            ┌──────────────┐
                 └──────┬───────┘            │   Postgres   │
                        ▼                    └──────┬───────┘
                 ┌──────────────┐                   ▼ WAL (logical)
                 │   Iceberg    │◀╌╌╌╌ Debezium ────┘
                 │  (curated,   │      topic: retail.cdc.public.products
                 │ checkpointed)│      (consumption path planned)
                 └──────────────┘
```

Solid today: the whole event path — **producer → Kafka → Spark → Iceberg** — plus
the CDC infrastructure (**Postgres → Debezium → Kafka**). What remains on the
[roadmap](#roadmap) is consuming the change-event topic into the curated layer.

### Event model

Two event types share the `retail.events` topic (same retail domain as
[spark-retail-etl](https://github.com/renatoaragon/spark-retail-etl)):

| Type    | Key        | Notable fields                                       |
|---------|------------|------------------------------------------------------|
| `sale`  | `order_id` | `category`, `quantity`, `unit_price`, `ts`           |
| `stock` | `sku`      | `category`, `warehouse`, `quantity_delta`, `ts`      |

Keying by the business id keeps a given order's or SKU's events ordered within a
partition — which the windowed aggregations downstream will rely on.

## Run the platform

```bash
docker compose up -d          # start Kafka (KRaft) + Schema Registry
docker compose ps             # wait for both to report healthy

# smoke-check
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list
curl -s http://localhost:8081/subjects        # Schema Registry, expect: []

docker compose down           # stop and remove
```

- **Kafka** (KRaft, no ZooKeeper) — host clients on `localhost:9092`.
- **Schema Registry** — `http://localhost:8081`.

Single-node and plaintext by design: replication factor 1, auto-topic-creation
off (topics are created explicitly), a fixed cluster id for stable storage across
restarts. CI validates `docker-compose.yml` on every change.

## Produce events

A seeded generator emits synthetic **sale** and **stock** events (same retail
domain as the batch project). The generator is pure and deterministic; the Kafka
send path is a thin, testable wrapper.

```bash
pip install -r requirements.txt

# create the topic (auto-creation is off), then produce
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create --topic retail.events --partitions 3

python -m retail_stream.producer --topic retail.events --count 500 --seed 42
```

Run the tests (no broker needed — the producer is exercised with a fake):

```bash
pytest -q
```

The suite ends with an end-to-end run on static data
([`test_pipeline.py`](tests/test_pipeline.py)): generator → producer wire format →
decode → valid/late routing → windowed revenue. It pins two properties no
single-stage test can see — the producer and consumer agree on the wire contract,
and every input record leaves the pipeline in exactly one place (output or DLQ).

## Consume events

A Spark Structured Streaming job subscribes to the topic, decodes the JSON payload
into typed columns, and (at this stage) prints them to the console.

```bash
python -m retail_stream.consumer --topic retail.events
```

The decode step (`parse_events`) is a plain `DataFrame -> DataFrame` transformation,
so it is unit-tested on a static DataFrame — only the streaming source and sink need
a live broker. Spark pulls the `spark-sql-kafka` package on first run.

## Windowed aggregations

Revenue per category over **event-time** windows, with a **watermark** so Spark can
bound lateness and evict state for windows that can no longer change.

```bash
python -m retail_stream.consumer --aggregate tumbling   # 1-min buckets
python -m retail_stream.consumer --aggregate sliding    # 5-min window, 1-min slide
```

- **Tumbling** — fixed, non-overlapping windows; each event lands in exactly one.
- **Sliding** — fixed-length windows advancing by a smaller slide, so they overlap
  and an event contributes to several (moving trends).
- **Watermark** (`event_time - 2 minutes`) — the threshold past which late events are
  dropped and completed windows' state is released; without it, streaming aggregation
  state would grow without bound.

`revenue_tumbling` / `revenue_sliding` are pure transforms, tested deterministically
on static data.

## Late data & dead-letter queue

Nothing is dropped silently. Records that can't be used as events are routed aside
with the original payload and a reason:

- **Malformed / unknown** (`split_valid_invalid`) — JSON that didn't parse, an
  unrecognised `event_type`, or an unreadable timestamp.
- **Late** (`split_on_time_late`) — well-formed but older than the watermark
  boundary, so a windowed aggregation would discard it.

`as_dlq` shapes those rows for a dead-letter sink (`event_id`, `raw_json`,
`dlq_reason`) — a separate Kafka topic or table you can inspect and replay. This is
why the decoder keeps the raw payload (`decode_with_raw`).

## Iceberg sink

The aggregated stream is written to an Apache **Iceberg** table, with a
**checkpoint** holding the Kafka offsets and streaming state so a restarted query
resumes exactly where it left off.

- `iceberg_configs` — Spark settings for a local Hadoop-catalog Iceberg setup.
- `configure_iceberg_writer` / `write_stream_to_iceberg` — wire a streaming writer
  to an Iceberg table with `checkpointLocation`.

The config and writer wiring are unit-tested with a fake writer; the actual write
needs the Iceberg runtime jar and a live stream, so it runs against the stack, not
in CI (mirroring how the broker itself is validated).

Checkpoint + Iceberg's atomic, batch-id-aware commits are what give the pipeline
**end-to-end exactly-once** delivery into the table — what that means precisely,
and where it stops, is documented in
[ADR 0001](docs/adr/0001-exactly-once-semantics.md).

## CDC: Postgres → Debezium → Kafka

The stream so far carries *events* (things that happened). CDC adds the other
source every real platform has: *state* — an operational OLTP table whose row
changes are captured from the write-ahead log and published to Kafka, with the
application that owns the table none the wiser.

The compose stack now includes:

- **`postgres`** — Postgres 16 running with `wal_level=logical` (the default
  `replica` level doesn't carry enough for CDC). An init script creates and seeds
  a `products` table — the state the sale/stock *events* act upon — and sets
  `REPLICA IDENTITY FULL` so update/delete events carry the full **before image**,
  not just the old key.
- **`connect`** — Kafka Connect with the Debezium Postgres connector, reading the
  WAL through the built-in `pgoutput` plugin (nothing extra to install in the
  database). Connect keeps its own config/offsets/status in Kafka.

Bring the stack up and register the connector
([`cdc/register-postgres.json`](cdc/register-postgres.json)):

```bash
docker compose up -d          # kafka, schema-registry, postgres, connect
curl -s -X POST -H "Content-Type: application/json" \
  --data @cdc/register-postgres.json http://localhost:8083/connectors | jq .
```

The initial snapshot publishes every existing row to
`retail.cdc.public.products`; from then on each INSERT/UPDATE/DELETE arrives as
a change event within milliseconds. Watch it happen:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic retail.cdc.public.products --from-beginning &

docker compose exec postgres psql -U retail -d retail \
  -c "UPDATE products SET unit_price = 13.90 WHERE sku = 'SKU0001'"
```

Because the broker runs with topic auto-creation off (deliberate, PR #1), the
connector declares its topic settings via Connect's `topic.creation.*` — the
change-event topic is created explicitly, same philosophy as the rest of the
stack. The registration document and init SQL are pinned by broker-free tests
(`tests/test_cdc_config.py`); the live path is exercised against the stack.

### Consuming the change events

The event pipeline is append-shaped; CDC is **update-shaped** — a change
supersedes the row's previous state, and a delete removes it. This is exactly
the case [ADR 0001](docs/adr/0001-exactly-once-semantics.md) reserved for
`foreachBatch` + `MERGE INTO`, and `retail_stream/cdc.py` implements it:

1. **Parse** the Debezium envelope (`before`/`after`/`op`/`ts_ms`), dropping
   Kafka log-compaction tombstones and garbage (the delete itself arrives as a
   regular `op='d'` event).
2. **Flatten** to one change per row — deletes take their key from `before`,
   since `after` is null.
3. **Compact** each micro-batch to the latest change per key (`ts_ms` order): a
   key touched five times in a batch has one true final state, and applying a
   stale intermediate last would corrupt the table.
4. **Merge** atomically into Iceberg: matched + `op='d'` → DELETE, matched →
   UPDATE, not matched (and not a delete) → INSERT.

Two connector settings exist purely for the consumer's sake:
`value.converter.schemas.enable=false` (the per-message schema envelope doubles
every payload) and `decimal.handling.mode=double` (Debezium's default encodes
`numeric(10,2)` as base64 bytes — the classic first-contact surprise).

```bash
python -c "from retail_stream.cdc import stream_cdc_to_iceberg"  # wiring lives here
```

Parsing, flattening, compaction and the MERGE statement are all tested on
static data (`tests/test_cdc.py`); the live merge needs the Iceberg runtime and
runs against the stack.

## Roadmap

- [x] Kafka (KRaft) + Schema Registry via Docker Compose
- [x] Producer of synthetic sales/stock events
- [x] Architecture overview + design notes
- [x] Spark Structured Streaming consumer
- [x] Windowed aggregations (tumbling + sliding) with watermarks
- [x] Late-data handling + dead-letter queue
- [x] Iceberg sink with checkpointing
- [x] Exactly-once semantics ([ADR 0001](docs/adr/0001-exactly-once-semantics.md))
- [x] CDC infrastructure: Postgres (logical decoding) + Debezium Connect
- [x] CDC consumption: change events → Spark → Iceberg (foreachBatch + MERGE)
- [x] ADR: [batch vs streaming vs CDC — when each is the right tool](docs/adr/0002-batch-vs-streaming-vs-cdc.md)

**The roadmap is complete.** Built stage by stage in 12 reviewed pull requests;
from here the repo is in maintenance: the PR history *is* the documentation of
how it grew.

## Design notes

- **KRaft, not ZooKeeper** — one process is both broker and controller. Fewer moving
  parts for a local lab, and the direction Kafka itself is heading.
- **Auto-topic-creation off** — topics are declared explicitly. A typo'd topic name
  fails loudly instead of silently spawning an empty topic.
- **Keyed events** — `order_id` / `sku` as the message key gives per-key ordering
  within a partition, the property windowed aggregations and any future joins need.
- **Deterministic generator** — seeded, wall-clock-free timestamps, so a run is
  reproducible and tests are stable. The data is synthetic; no real data is used.
- **Client-agnostic send path** — serialization and the produce loop don't import
  the Kafka client, so they're unit-tested with a fake and the native library is
  only needed to actually talk to a broker.
- **JSON first, Avro next** — the wire format is JSON today; registering Avro schemas
  with the Schema Registry lands once the consumer exists to read against them.

Each stage is a separate PR with a short design note, so the history reads as a
sequence of decisions. Where a decision is heavier (exactly-once, CDC), it gets
its own ADR under [`docs/adr/`](docs/adr/).

## License

MIT — see [LICENSE](LICENSE).
