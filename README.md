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
                 ┌──────────────┐                   ▼ CDC
                 │   Iceberg    │◀──── Debezium ──── (planned)
                 │  (curated,   │
                 │ checkpointed)│
                 └──────────────┘
```

Solid today: **producer → Kafka**. The Spark consumer, Iceberg sink, and the CDC
branch are the stages still on the [roadmap](#roadmap) below; the diagram shows
where they plug in.

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

## Roadmap

- [x] Kafka (KRaft) + Schema Registry via Docker Compose
- [x] Producer of synthetic sales/stock events
- [x] Architecture overview + design notes
- [x] Spark Structured Streaming consumer
- [x] Windowed aggregations (tumbling + sliding) with watermarks
- [x] Late-data handling + dead-letter queue
- [x] Iceberg sink with checkpointing
- [x] Exactly-once semantics ([ADR 0001](docs/adr/0001-exactly-once-semantics.md))
- [ ] CDC: Postgres + Debezium → Kafka → Iceberg

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
