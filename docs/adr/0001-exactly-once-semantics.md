# 1. Exactly-once semantics: what this pipeline guarantees, and how

- Status: Accepted
- Date: 2026-07

## Context

Streaming queries restart — a crash, a deploy, a node lost mid-batch. Whoever
reads the revenue table then has to ask: after that restart, are these numbers
**missing** events (at-most-once) or **double-counting** them (at-least-once)?
Neither is acceptable for a table people aggregate money over.

The naive answers both fail. Don't retry after a failure and the half-processed
batch is lost. Blindly retry and every event in it lands twice — and a windowed
`SUM(revenue)` (PR #5) silently inflates.

One misconception to clear up front: exactly-once does **not** mean "each event
is *processed* exactly once". Reprocessing after a failure is unavoidable in any
retry-based system. The achievable guarantee — sometimes called
*effectively-once* — is that the **effects visible in the sink** are as if each
event had been processed once, no matter how many times a batch was re-executed
to get there.

## Decision

Rely on the classic three-part contract for end-to-end exactly-once, with each
part supplied by a layer this pipeline already has:

1. **Replayable source — Kafka.** The topic is a log, not a destructive queue:
   any offset range can be re-read after a failure. Consumer offsets are *not*
   committed back to Kafka as the source of truth; Spark owns them (below).
2. **Deterministic re-execution from a checkpoint — Spark.** The
   `checkpointLocation` (PR #7) write-ahead-logs each micro-batch's exact offset
   range *before* executing it, alongside the aggregation state. On restart,
   Spark replans the **same batch over the same offsets**, so a retry is a
   replay of identical input, not a guess.
3. **Idempotent, atomic sink — Iceberg.** A micro-batch commits as one atomic
   snapshot, and the writer records the streaming query id and batch (epoch) id
   in the snapshot summary. When a recovered query re-executes an
   already-committed batch, the sink recognises the batch id and **skips the
   duplicate commit**. Readers see whole batches or nothing — never a torn one.

Remove any leg and the guarantee degrades: a non-replayable source loses data, no
checkpoint means re-reading from an arbitrary position, and a non-transactional
sink (the console sink, a plain Kafka DLQ topic) double-writes on retry.

## Consequences

**Positive**

- The revenue table is restart-safe. Kill the query mid-batch, restart it, and
  the table converges to the same rows — no manual reconciliation after incidents.
- The guarantee is **inherited from the architecture**, not implemented here: no
  custom dedup bookkeeping to maintain or get subtly wrong.
- Failure handling is boring by design: restart the query, done.

**Negative / boundaries — what this does *not* guarantee**

- **Scoped to one query + one checkpoint.** Delete or change
  `checkpointLocation` and it becomes a *new* query: Iceberg no longer
  recognises prior batch ids, and the stream reprocesses from its configured
  starting offsets — duplicating history. The checkpoint is operational state
  and must be treated like data.
- **Ends at producer-generated duplicates.** Our producer (PR #2) is plain
  at-least-once: if it retried a send, the topic would hold two *distinct
  records* with the same `event_id` — real input as far as the sink is
  concerned, not a delivery duplicate it can suppress. De-duplicating those
  takes a business key (`dropDuplicates` within the watermark, or a `MERGE` —
  see alternatives).
- **Side outputs are weaker.** The console sink and a DLQ topic are
  at-least-once: a replayed batch re-emits its rows. Acceptable here — the DLQ
  (PR #6) exists for manual inspection and replay — but worth stating.
- **Latency floor.** Transactional commit per micro-batch ties end-to-end
  latency to the trigger interval. Spark's lower-latency continuous mode only
  offers at-least-once, so this pipeline deliberately stays micro-batch.

## Alternatives considered

- **`foreachBatch` + `MERGE INTO` on a business key** — de-duplicates even
  source-generated duplicates and handles *updates*, at the cost of writing and
  maintaining the merge ourselves. Unnecessary for today's append-only
  aggregates; becomes the right tool when CDC lands (next roadmap stage), since
  CDC is update-shaped by nature.
- **Kafka transactions (EOS), `read_committed` consumers** — Kafka's native
  exactly-once, the right answer for Kafka→Kafka topologies (Kafka Streams).
  Our terminal store is a table, not a topic, so the transactional edge belongs
  to the table format instead.
- **Accept at-least-once + a dedup view downstream** — cheapest pipeline,
  but it pushes correctness onto every consumer forever. Rejected as a default
  posture, and unnecessary given the sink already provides idempotent commits.
