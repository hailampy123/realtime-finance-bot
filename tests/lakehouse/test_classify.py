from __future__ import annotations

import datetime as dt

import pytest

from lakehouse.trades.schema import QUARANTINE_REASON
from lakehouse.trades.transforms import classify_trades, decode_kafka_trades

_KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _reason(spark, avro_bytes, record):
    df = spark.createDataFrame(
        [
            (
                bytearray(b"k"),
                bytearray(avro_bytes(record)),
                "md.trades.v1",
                0,
                1,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
        _KAFKA_SCHEMA,
    )
    return classify_trades(decode_kafka_trades(df)).collect()[0][QUARANTINE_REASON]


def test_a_good_trade_has_no_reason(spark, trade_record, avro_bytes):
    assert _reason(spark, avro_bytes, trade_record()) is None


def test_corrupt_datum_is_decode_failed(spark):
    df = spark.createDataFrame(
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
        _KAFKA_SCHEMA,
    )
    out = classify_trades(decode_kafka_trades(df)).collect()[0]
    assert out[QUARANTINE_REASON] == "decode_failed"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"venue": ""}, "missing_key"),
        ({"trade_id": ""}, "missing_key"),
        ({"instrument_id": ""}, "missing_instrument"),
        # A 2026 timestamp left in milliseconds -- the §6.3 unit trap.
        ({"event_ts_us": 1_786_000_000_000}, "bad_timestamp"),
        ({"price": "not-a-number"}, "bad_price"),
        ({"price": "0"}, "bad_price"),
        ({"price": "-1.5"}, "bad_price"),
        ({"size": "not-a-number"}, "bad_size"),
        ({"size": "0"}, "bad_size"),
    ],
)
def test_each_rejection_reports_its_own_reason(
    spark, trade_record, avro_bytes, overrides, expected
):
    assert _reason(spark, avro_bytes, trade_record(**overrides)) == expected


def test_future_timestamp_beyond_one_day_is_rejected(spark, trade_record, avro_bytes):
    far_future = int((dt.datetime.now(dt.UTC).timestamp() + 3 * 86_400) * 1_000_000)
    assert _reason(spark, avro_bytes, trade_record(event_ts_us=far_future)) == "bad_timestamp"


def test_first_matching_reason_wins(spark, trade_record, avro_bytes):
    # Both the key and the price are broken; the more fundamental one reports.
    record = trade_record(trade_id="", price="nope")
    assert _reason(spark, avro_bytes, record) == "missing_key"


def test_high_precision_price_is_not_rejected(spark, trade_record, avro_bytes):
    # 18 fractional digits must survive; rejecting it would silently drop
    # legitimate small-tick instruments.
    assert _reason(spark, avro_bytes, trade_record(price="1.234567890123456789")) is None
