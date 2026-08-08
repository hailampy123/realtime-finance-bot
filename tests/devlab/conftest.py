from __future__ import annotations

from typing import Any

import pytest

BASE_TS_US = 1_700_000_000_000_000  # 2023-11-14T22:13:20Z


def record(
    *,
    venue: str = "binance",
    venue_symbol: str = "BTCUSDT",
    instrument_id: str = "BTC-USD",
    trade_id: str = "1",
    price: str = "100.00",
    size: str = "1.0",
    side: str = "BUY",
    sequence: int | None = 1,
    offset_us: int = 0,
    source: str = "STREAM",
    is_backfill: bool = False,
    kafka_offset: int = 0,
) -> dict[str, Any]:
    """One decoded trade, shaped exactly as devlab.stream.tail yields it."""
    event_ts_us = BASE_TS_US + offset_us
    return {
        "venue": venue,
        "venue_symbol": venue_symbol,
        "instrument_id": instrument_id,
        "trade_id": trade_id,
        "event_ts_us": event_ts_us,
        "ingest_ts_us": event_ts_us + 250_000,
        "price": price,
        "size": size,
        "side": side,
        "sequence": sequence,
        "is_backfill": is_backfill,
        "source": source,
        "kafka_partition": 0,
        "kafka_offset": kafka_offset,
        "kafka_timestamp_ms": event_ts_us // 1000,
        "kafka_key": f"{venue}|{venue_symbol}",
    }


@pytest.fixture
def make_record():
    return record
