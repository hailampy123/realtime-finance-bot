from __future__ import annotations

import json

from lakehouse.trades import schema


def test_avsc_path_points_at_the_producer_schema():
    # Single source of truth: the consumer must read the very file the producer
    # encodes with, or the "drift is impossible" property in codec.py is lost.
    path = schema.trade_avsc_path()
    assert path.exists()
    assert path.as_posix().endswith("ingest/schemas/trade.v1.avsc")


def test_avsc_json_parses_and_declares_the_expected_fields():
    doc = json.loads(schema.trade_avsc_json())
    assert doc["name"] == "Trade"
    names = [f["name"] for f in doc["fields"]]
    assert names == [
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        "ingest_ts_us",
        "price",
        "size",
        "side",
        "sequence",
        "is_backfill",
        "source",
    ]


def test_epoch_floor_rejects_millisecond_timestamps():
    # A 2026 timestamp in milliseconds is ~1.78e12, three orders of magnitude
    # below the microsecond floor. This constant is the ms/us tripwire.
    ms_style = 1_786_000_000_000
    assert ms_style < schema.EPOCH_FLOOR_US


def test_silver_columns_exclude_bronze_audit_columns():
    assert set(schema.SILVER_COLUMNS).isdisjoint(schema.BRONZE_AUDIT_COLUMNS)
