"""Bronze + Silver trades pipeline (stage 2a).

    Kafka -> bronze_trades_stream -> trades_validated -> silver_trades
                                                     \\-> silver_trades_quarantine

This file is a shell. All logic lives in lakehouse.trades.transforms so it can be
tested without Databricks; see tests/lakehouse. `pyspark.pipelines` only exists
on Databricks Runtime, so nothing here is importable under pytest, and
tests/lakehouse/test_pipeline_contract.py pins these values by parsing instead.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from lakehouse.trades.transforms import (
    QUARANTINE_PREDICATES,
    classify_trades,
    decode_kafka_trades,
    quarantined_trades,
    valid_trades,
)

spark = SparkSession.active()

# Pinned rather than inherited. Every event_ts in Silver is derived from
# microseconds, so a workspace whose session zone was not UTC would shift the
# whole series. Databricks already defaults to UTC; this makes it explicit.
spark.conf.set("spark.sql.session.timeZone", "UTC")

_SECRET_SCOPE = spark.conf.get("fdai.secret_scope", "fdai")
_TOPIC = spark.conf.get("fdai.kafka_topic", "md.trades.v1")
_STARTING_OFFSETS = spark.conf.get("fdai.starting_offsets", "earliest")
_MAX_OFFSETS_PER_TRIGGER = spark.conf.get("fdai.max_offsets_per_trigger", "1000000")


def _kafka_options() -> dict[str, str]:
    """SASL/SCRAM against the MSK public listener.

    The login-module class name must carry the `kafkashaded.` prefix: Databricks
    shades its Kafka client, and the unshaded name fails with
    RESTRICTED_STREAMING_OPTION_PERMISSION_ENFORCED.
    """
    username = dbutils.secrets.get(_SECRET_SCOPE, "kafka_username")  # noqa: F821
    password = dbutils.secrets.get(_SECRET_SCOPE, "kafka_password")  # noqa: F821
    bootstrap = dbutils.secrets.get(_SECRET_SCOPE, "kafka_bootstrap")  # noqa: F821
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{username}" password="{password}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": _TOPIC,
        "startingOffsets": _STARTING_OFFSETS,
        "maxOffsetsPerTrigger": _MAX_OFFSETS_PER_TRIGGER,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": jaas,
        # Left at the default `true` on purpose: silently skipping expired
        # offsets would hide real data loss. Recovery after the weekly sandbox
        # wipe is a SELECTIVE refresh of bronze_trades_stream -- never the whole
        # pipeline, which would also reset silver_trades. See
        # docs/RUNBOOK_STAGE_2A.md.
        "failOnDataLoss": "true",
    }


@dp.table(
    name="bronze_trades_stream",
    comment="Raw md.trades.v1: decoded Avro plus Kafka metadata and the original bytes.",
)
def bronze_trades_stream() -> DataFrame:
    raw = spark.readStream.format("kafka").options(**_kafka_options()).load()
    return decode_kafka_trades(raw)


# Warn-only, so no row is ever dropped here -- routing is done by the
# _quarantine_reason column. These exist so the quarantine rate shows up in the
# pipeline's data-quality metrics, which is what the SLI is defined against.
_EXPECTATIONS = {
    f"valid_{reason}": predicate for reason, predicate in QUARANTINE_PREDICATES.items()
}


@dp.temporary_view(name="trades_validated")
@dp.expect_all(_EXPECTATIONS)
def trades_validated() -> DataFrame:
    return classify_trades(spark.readStream.table("bronze_trades_stream"))


@dp.table(
    name="silver_trades_quarantine",
    comment="Rejected records with the reason and the raw bytes needed to diagnose them.",
)
def silver_trades_quarantine() -> DataFrame:
    return quarantined_trades(spark.readStream.table("trades_validated"))


@dp.temporary_view(name="trades_clean")
def trades_clean() -> DataFrame:
    return valid_trades(spark.readStream.table("trades_validated"))


dp.create_streaming_table(
    name="silver_trades",
    comment="Deduplicated trade facts, keyed upsert on (venue, trade_id).",
    table_properties={
        # Stage 2b reads this to find dirty partitions. It must be on from the
        # first commit; enabling it later does not backfill change data.
        "delta.enableChangeDataFeed": "true",
    },
)

# Named flow from day one so stage 3a can add `cdc_trades_archive` into the same
# target without touching this one. SCD Type 1 is safe here because a trade is
# immutable: the stream and archive copies tie on event_ts_us.
#
# No except_column_list: trades_clean (via transforms.valid_trades) already
# projects down to exactly the Silver contract columns, so there is nothing
# left to exclude. A live run on 2026-08-13 proved that adding it anyway is not
# redundant but wrong -- except_column_list tries to resolve the named columns
# against the source, and none of the Kafka audit columns exist in trades_clean
# to resolve, which failed the flow with UNRESOLVED_COLUMN before a single
# record was processed.
dp.create_auto_cdc_flow(
    name="cdc_trades_stream",
    target="silver_trades",
    source="trades_clean",
    keys=["venue", "trade_id"],
    sequence_by="event_ts_us",
    stored_as_scd_type=1,
)
