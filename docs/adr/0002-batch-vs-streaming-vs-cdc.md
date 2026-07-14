# 2. Batch vs streaming vs CDC: when each is the right tool

- Status: Accepted
- Date: 2026-07

## Context

This repo and its batch sibling
([spark-retail-etl](https://github.com/renatoaragon/spark-retail-etl)) now
cover the three ways data moves through a platform: **batch** (scheduled reads
of accumulated data), **streaming** (events processed as they happen), and
**CDC** (row changes captured from an operational database). Having built all
three over the same retail domain, the closing question deserves its own
record — because the most common architecture mistake is not building any of
them badly; it is picking the wrong one and executing it well.

## Decision

Choose by asking two questions about the *data* — never by starting from the
technology:

**1. Is the source a fact that happened, or state that changes?**

- Facts (a sale, a click, a sensor reading) are **events**: immutable,
  append-shaped, naturally a log. They suit batch or streaming.
- State (a product's price, a customer's address) is **update-shaped**: the
  latest version supersedes the rest, deletes matter. If the system that owns
  it cannot publish events — most operational databases can't — **CDC** is how
  its changes become a stream without touching the owning application
  (no dual-writes, no polling, no outbox negotiation).

**2. How stale is too stale?**

- If tomorrow morning is fine, **batch** wins: simplest to build, test,
  backfill and reason about. Its failure mode is a rerun, not an incident.
  Incremental loads with a watermark (the sibling repo) keep cost proportional
  to new data.
- If the answer is minutes-to-seconds, **streaming** earns its complexity:
  event-time windows, watermarks, late data, state stores, exactly-once sinks
  (ADR 0001) — every one of which this repo had to build before the first
  metric was trustworthy.

The combinations are the real-world answer: CDC feeding a streaming pipeline
(this repo's `products` branch), streams landing in a lakehouse that batch
jobs then aggregate (Iceberg is the meeting point), batch backfilling what a
stream got wrong.

## Consequences

**Positive**

- The default is explicit: **start with batch, and let latency requirements —
  not enthusiasm — promote a pipeline to streaming.** A streaming pipeline
  nobody needed is the most expensive way to compute yesterday's numbers.
- CDC is scoped to what it is: a *source* technique (state → stream), not a
  third processing paradigm. Once changes are on the topic, they are consumed
  with the same streaming machinery (foreachBatch + MERGE, this repo).
- The costs are named before they are paid: a streaming pipeline carries
  watermark tuning, DLQ operations, checkpoint custody and state-store sizing
  for its entire life. Those costs bought throughput/cycle-time metrics in
  minutes instead of tomorrow — a trade that is only worth stating because it
  sometimes isn't.

**Negative / trade-offs**

- "Batch first" means some pipelines are rebuilt later when latency
  requirements tighten. Accepted: rebuilding a well-understood batch pipeline
  as streaming is far cheaper than operating a speculative streaming pipeline
  that never needed to exist.
- CDC couples the pipeline to the source's schema; a column rename upstream is
  a breaking change downstream. Mitigations (schema registry, contract tests)
  are their own investment — budgeted, not free.
- Running all three means three failure vocabularies on call: reruns (batch),
  lag and state (streaming), replication slots and snapshots (CDC).

## Alternatives considered

- **Streaming everything** ("the lambda killer") — one paradigm, no dual code
  paths. Rejected: for data that tolerates hours of latency, it trades the
  cheapest-to-operate model for the most expensive one and gets nothing back.
- **Batch everything, faster** (micro-batch every 5 minutes) — honest and often
  right, but it cannot express per-event semantics (a DLQ for one malformed
  record, per-key ordering) and rerunning ever-smaller windows converges on a
  worse streaming engine.
- **Polling the operational database instead of CDC** — simpler on day one,
  then misses deletes, hammers the source with `WHERE updated_at > ?` scans,
  and silently skips rows whose clock lies. The WAL already knows every change
  in order; logical decoding just reads it.
