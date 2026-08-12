"""Pure PySpark transforms for the trades Bronze/Silver path.

Every function here takes and returns a DataFrame and touches no Databricks-only
API, which is what lets the whole path be tested on a laptop. The declarative
pipeline in lakehouse/pipelines/trades.py is a thin shell over these.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from lakehouse.trades.schema import (
    DECIMAL_TYPE,
    EPOCH_FLOOR_US,
    ONE_DAY_US,
    QUARANTINE_REASON,
    trade_avsc_json,
)

# The producer writes a bare Avro datum via fastavro.schemaless_writer, with no
# Confluent magic-byte prefix and no schema registry, which is exactly what
# from_avro's jsonFormatSchema form consumes. See ingest/core/codec.py.
#
# PERMISSIVE, not the default FAILFAST: FAILFAST aborts the entire micro-batch
# on a single malformed record, so one poison message halts ingestion forever.
# PERMISSIVE yields a NULL struct, which classify_trades turns into a
# quarantined row.
_AVRO_OPTIONS = {"mode": "PERMISSIVE"}

_DECODED = "_decoded"


def decode_kafka_trades(df: DataFrame) -> DataFrame:
    """Decode Kafka's binary `value` into flat trade columns, keeping audit data."""
    return df.withColumn(
        _DECODED, from_avro(F.col("value"), trade_avsc_json(), _AVRO_OPTIONS)
    ).select(
        F.col(f"{_DECODED}.*"),
        F.col("value").alias("_kafka_value"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )


# Each entry maps a reason name to the SQL predicate that must hold for a row to
# be VALID. Order matters: the first failing predicate names the reason, so the
# most fundamental problem is reported rather than a downstream symptom.
#
# Every predicate is NULL-safe by construction. SQL is three-valued, so a naive
# `try_cast(price AS DECIMAL) > 0` yields NULL (not FALSE) for an unparseable
# price, and `NOT NULL` is NULL, which would silently fail to match and let a
# junk price through as valid. Each numeric check therefore asserts IS NOT NULL
# before comparing.
#
# The pipeline shell reuses these as warn-only expectations, which keeps the
# quality metrics behind the quarantine-rate SLI meaningful while the actual
# routing is done by the reason column.
QUARANTINE_PREDICATES: dict[str, str] = {
    # All three are NULL together only when PERMISSIVE from_avro failed to
    # decode. `IS NOT NULL` returns FALSE rather than NULL, so this is safe.
    "decode_failed": "venue IS NOT NULL OR trade_id IS NOT NULL OR event_ts_us IS NOT NULL",
    # AUTO CDC keys cannot be NULL, so this is a pipeline error, not a warning.
    "missing_key": (
        "venue IS NOT NULL AND venue <> '' AND trade_id IS NOT NULL AND trade_id <> ''"
    ),
    "missing_instrument": "instrument_id IS NOT NULL AND instrument_id <> ''",
    "bad_timestamp": (
        f"event_ts_us IS NOT NULL AND event_ts_us >= {EPOCH_FLOOR_US} "
        f"AND event_ts_us <= unix_micros(current_timestamp()) + {ONE_DAY_US}"
    ),
    "bad_price": (
        f"try_cast(price AS {DECIMAL_TYPE}) IS NOT NULL "
        f"AND try_cast(price AS {DECIMAL_TYPE}) > 0"
    ),
    "bad_size": (
        f"try_cast(size AS {DECIMAL_TYPE}) IS NOT NULL "
        f"AND try_cast(size AS {DECIMAL_TYPE}) > 0"
    ),
    "bad_side": "side IS NOT NULL AND side IN ('BUY', 'SELL', 'UNKNOWN')",
}


def classify_trades(df: DataFrame) -> DataFrame:
    """Add a nullable `_quarantine_reason`; NULL means the row is valid.

    Chained `when` clauses give first-match-wins, and the deliberate absence of
    an `otherwise` is what makes a valid row's reason NULL.
    """
    reason = None
    for name, valid_when in QUARANTINE_PREDICATES.items():
        condition = ~F.expr(valid_when)
        reason = (
            F.when(condition, F.lit(name))
            if reason is None
            else reason.when(condition, F.lit(name))
        )
    return df.withColumn(QUARANTINE_REASON, reason)


def valid_trades(df: DataFrame) -> DataFrame:
    """The clean branch: typed, projected to the Silver contract, nothing extra.

    Kafka audit columns are dropped here rather than only by the CDC flow's
    except_column_list, so the projection is testable without Databricks. The
    flow still declares the exclusions, which is belt and braces.
    """
    return df.where(F.col(QUARANTINE_REASON).isNull()).select(
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        F.timestamp_micros(F.col("event_ts_us")).alias("event_ts"),
        "ingest_ts_us",
        F.col("price").cast(DECIMAL_TYPE).alias("price"),
        F.col("size").cast(DECIMAL_TYPE).alias("size"),
        "side",
        "source",
        "sequence",
        "is_backfill",
    )


def quarantined_trades(df: DataFrame) -> DataFrame:
    """The rejected branch: everything, plus why it was rejected.

    Deliberately keeps `_kafka_value` -- a quarantined record that cannot be
    re-read is a lost record with a receipt.
    """
    return df.where(F.col(QUARANTINE_REASON).isNotNull())
