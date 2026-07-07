"""Late-data handling and a dead-letter queue (DLQ).

Two ways a record can fail to be a usable event, each routed aside instead of
silently dropped:

- **Malformed / unknown** — the JSON did not parse, or the ``event_type`` is not one
  we recognise, or the timestamp was unreadable. These are structurally unusable.
- **Late** — the record is well-formed but its ``event_time`` is older than the
  watermark boundary, so a windowed aggregation would have discarded it.

Both paths produce DLQ records that keep the original payload and a ``dlq_reason``,
so nothing is lost and the cause is inspectable. All functions are pure
``DataFrame`` transforms, tested on static data.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KNOWN_TYPES = ("sale", "stock")

REASON_MALFORMED = "malformed_or_unknown"
REASON_LATE = "late"


def split_valid_invalid(decoded: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition decoded records into (valid, invalid).

    Invalid = JSON that failed to parse (all fields null), an unrecognised
    ``event_type``, or an unparseable timestamp (null ``event_time``).
    """
    is_valid = (
        F.col("event_type").isin(list(KNOWN_TYPES))
        & F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
    )
    valid = decoded.filter(is_valid)
    invalid = decoded.filter(~is_valid | is_valid.isNull())
    return valid, invalid


def split_on_time_late(
    events: DataFrame, watermark_ts
) -> tuple[DataFrame, DataFrame]:
    """Partition events into (on_time, late) around a watermark boundary.

    ``watermark_ts`` is the cutoff (in a real pipeline, ``max(event_time) - delay``);
    events strictly before it are late and would be dropped by a windowed
    aggregation, so they are routed to the DLQ instead.
    """
    on_time = events.filter(F.col("event_time") >= F.lit(watermark_ts))
    late = events.filter(F.col("event_time") < F.lit(watermark_ts))
    return on_time, late


def as_dlq(records: DataFrame, reason: str) -> DataFrame:
    """Shape rows for the dead-letter sink: an id, the raw payload, and a reason."""
    raw_col = (
        F.col("raw_json") if "raw_json" in records.columns else F.to_json(F.struct("*"))
    )
    return records.select(
        F.coalesce(F.col("event_id"), F.lit("unknown")).alias("event_id"),
        raw_col.alias("raw_json"),
        F.lit(reason).alias("dlq_reason"),
    )
