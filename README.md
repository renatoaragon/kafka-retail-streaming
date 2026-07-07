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

## Planned architecture

```
 producer ──▶ Kafka (KRaft) ──▶ Spark Structured Streaming ──▶ Iceberg
 (sales/stock   + Schema         windowed aggregations,          (curated,
  events)        Registry        watermarks, late data, DLQ       checkpointed)
                                          ▲
 Postgres ──▶ Debezium ────────────────────  (CDC source)
```

## Roadmap

- [ ] Kafka (KRaft) + Schema Registry via Docker Compose
- [ ] Producer of synthetic sales/stock events
- [ ] Spark Structured Streaming consumer
- [ ] Windowed aggregations (tumbling + sliding) with watermarks
- [ ] Late-data handling + dead-letter queue
- [ ] Iceberg sink with checkpointing
- [ ] Exactly-once semantics (documented as an ADR)
- [ ] CDC: Postgres + Debezium → Kafka → Iceberg

## License

MIT — see [LICENSE](LICENSE).
