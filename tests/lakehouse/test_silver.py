from __future__ import annotations

import pytest

pytest.importorskip("pyspark", reason="needs `uv sync --group lakehouse`")

import datetime as dt
from decimal import Decimal

from lakehouse.trades.schema import QUARANTINE_REASON, SILVER_COLUMNS
from lakehouse.trades.transforms import (
    classify_trades,
    decode_kafka_trades,
    quarantined_trades,
    valid_trades,
)

_KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _classified(spark, avro_bytes, records):
    rows = [
        (
            bytearray(b"k"),
            bytearray(avro_bytes(r)),
            "md.trades.v1",
            0,
            i,
            dt.datetime(2026, 8, 12, 1, 2, 3),
        )
        for i, r in enumerate(records)
    ]
    return classify_trades(decode_kafka_trades(spark.createDataFrame(rows, _KAFKA_SCHEMA)))


def test_silver_projects_exactly_the_contract_columns(spark, trade_record, avro_bytes):
    out = valid_trades(_classified(spark, avro_bytes, [trade_record()]))
    assert out.columns == SILVER_COLUMNS


def test_silver_drops_kafka_metadata(spark, trade_record, avro_bytes):
    # Carrying offsets into Silver would break the interchangeability argument
    # that makes SCD Type 1 safe for the archive backfill (design §2.3).
    out = valid_trades(_classified(spark, avro_bytes, [trade_record()]))
    assert not [c for c in out.columns if c.startswith("_kafka")]
    assert QUARANTINE_REASON not in out.columns


def test_price_and_size_become_exact_decimals(spark, trade_record, avro_bytes):
    record = trade_record(price="1.234567890123456789", size="0.000000000000000001")
    row = valid_trades(_classified(spark, avro_bytes, [record])).collect()[0]
    assert row["price"] == Decimal("1.234567890123456789")
    assert row["size"] == Decimal("0.000000000000000001")


def test_event_ts_is_derived_from_microseconds(spark, trade_record, avro_bytes):
    # Asserted via Spark rather than the collected Python value: pyspark renders
    # a TIMESTAMP into a naive datetime in the *driver's* local zone, which would
    # make this test pass or fail based on where it runs. The instant itself is
    # what matters, so check the exact round trip plus the UTC rendering.
    from pyspark.sql import functions as F

    record = trade_record(event_ts_us=1_700_000_000_000_000)
    row = (
        valid_trades(_classified(spark, avro_bytes, [record]))
        .select(
            "event_ts_us",
            F.unix_micros(F.col("event_ts")).alias("round_trip_us"),
            F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss").alias("rendered"),
        )
        .collect()[0]
    )
    assert row["event_ts_us"] == 1_700_000_000_000_000
    # No drift: the derived timestamp is exactly the microseconds it came from.
    assert row["round_trip_us"] == 1_700_000_000_000_000
    # And the session renders it in UTC, matching the project's stated contract.
    assert row["rendered"] == "2023-11-14 22:13:20"


def test_invalid_rows_are_absent_from_silver_and_present_in_quarantine(
    spark, trade_record, avro_bytes
):
    good = trade_record(trade_id="good")
    bad = trade_record(trade_id="bad", price="nope")
    classified = _classified(spark, avro_bytes, [good, bad])

    silver_ids = [r["trade_id"] for r in valid_trades(classified).collect()]
    assert silver_ids == ["good"]

    quarantined = quarantined_trades(classified).collect()
    assert len(quarantined) == 1
    assert quarantined[0]["trade_id"] == "bad"
    assert quarantined[0][QUARANTINE_REASON] == "bad_price"
    # Nothing is dropped: the raw bytes are still there to diagnose.
    assert quarantined[0]["_kafka_value"] is not None


def test_quarantine_keeps_the_reason_column(spark, trade_record, avro_bytes):
    classified = _classified(spark, avro_bytes, [trade_record(size="0")])
    assert QUARANTINE_REASON in quarantined_trades(classified).columns
