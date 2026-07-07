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

## Roadmap

- [x] Kafka (KRaft) + Schema Registry via Docker Compose
- [x] Producer of synthetic sales/stock events
- [x] Architecture overview + design notes
- [x] Spark Structured Streaming consumer
- [ ] Windowed aggregations (tumbling + sliding) with watermarks
- [ ] Late-data handling + dead-letter queue
- [ ] Iceberg sink with checkpointing
- [ ] Exactly-once semantics (documented as an ADR)
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
sequence of decisions. Where a decision is heavier (exactly-once, CDC), it will get
its own ADR under `docs/adr/`.

## License

MIT — see [LICENSE](LICENSE).
