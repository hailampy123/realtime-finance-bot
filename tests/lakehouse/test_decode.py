from __future__ import annotations

import datetime as dt

from lakehouse.trades.transforms import decode_kafka_trades

_KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _kafka_frame(spark, rows):
    """Mimic the shape spark.readStream.format('kafka') produces."""
    return spark.createDataFrame(rows, _KAFKA_SCHEMA)


def test_decodes_a_real_producer_datum(spark, trade_record, avro_bytes):
    rec = trade_record(trade_id="99", price="12345.678", side="SELL")
    df = _kafka_frame(
        spark,
        [
            (
                bytearray(b"binance|BTCUSDT"),
                bytearray(avro_bytes(rec)),
                "md.trades.v1",
                3,
                1234,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
    )
    out = decode_kafka_trades(df).collect()[0]

    assert out["venue"] == "binance"
    assert out["trade_id"] == "99"
    assert out["price"] == "12345.678"  # still a string in Bronze
    assert out["side"] == "SELL"
    assert out["source"] == "STREAM"
    assert out["sequence"] == 7
    assert out["is_backfill"] is False
    # Kafka metadata is preserved for forensics.
    assert out["_kafka_topic"] == "md.trades.v1"
    assert out["_kafka_partition"] == 3
    assert out["_kafka_offset"] == 1234
    assert out["_kafka_key"] == "binance|BTCUSDT"
    assert out["_ingested_at"] is not None


def test_corrupt_datum_yields_nulls_instead_of_failing(spark):
    # PERMISSIVE is the whole point: one poison record must not abort the batch.
    df = _kafka_frame(
        spark,
        [
            (
                bytearray(b"k"),
                bytearray(b"\xff\xff\xff\xff\xff\xff"),
                "md.trades.v1",
                0,
                1,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
    )
    out = decode_kafka_trades(df).collect()[0]

    assert out["venue"] is None
    assert out["trade_id"] is None
    # The raw bytes survive so the record stays diagnosable.
    assert out["_kafka_value"] is not None
