from __future__ import annotations

import pytest

# pandas ships in the opt-in `notebook` group, so this module skips under a
# plain `make test`. `make notebook-test` runs it with the group installed.
pytest.importorskip("pandas")

from devlab import frames
from devlab.health import PartitionInfo
from tests.devlab.conftest import record


def test_an_empty_read_yields_an_empty_frame_not_a_crash():
    df = frames.trades_frame([])
    assert df.empty
    assert "price" in df.columns


def test_strings_become_numbers_but_the_originals_survive():
    df = frames.trades_frame([record(price="43210.55", size="0.0012")])
    assert df["price"].dtype == "float64"
    assert df.loc[0, "price"] == pytest.approx(43210.55)
    # The exact decimal is what to check when a float looks wrong.
    assert df.loc[0, "price_str"] == "43210.55"
    assert df.loc[0, "size_str"] == "0.0012"


def test_timestamps_become_utc_datetimes():
    df = frames.trades_frame([record()])
    # Microsecond resolution, matching event_ts_us on the wire — pandas 2.x
    # keeps the unit it was given rather than upcasting everything to ns.
    assert str(df["event_ts"].dtype) == "datetime64[us, UTC]"
    assert df.loc[0, "event_ts"].year == 2023


def test_latency_is_derived_from_the_two_timestamps():
    # conftest sets ingest_ts 250_000us after event_ts.
    df = frames.trades_frame([record()])
    assert df.loc[0, "latency_ms"] == pytest.approx(250.0)


def test_rows_come_back_in_event_time_order():
    records = [
        record(trade_id="late", offset_us=5_000_000),
        record(trade_id="early", offset_us=0),
    ]
    df = frames.trades_frame(records)
    assert list(df["trade_id"]) == ["early", "late"]


def test_notional_is_price_times_size():
    df = frames.trades_frame([record(price="100", size="2.5")])
    assert df.loc[0, "notional"] == pytest.approx(250.0)


def test_dedupe_collapses_the_natural_key():
    # The same trade redelivered by REST repair is one trade, not two.
    records = [
        record(trade_id="777", source="STREAM", kafka_offset=1),
        record(trade_id="777", source="REST_REPAIR", kafka_offset=9),
        record(trade_id="778", kafka_offset=2),
    ]
    df = frames.dedupe(frames.trades_frame(records))
    assert len(df) == 2
    assert sorted(df["trade_id"]) == ["777", "778"]


def test_dedupe_keeps_the_earliest_arrival():
    records = [
        record(trade_id="777", source="STREAM", kafka_offset=1),
        record(trade_id="777", source="REST_REPAIR", kafka_offset=9),
    ]
    df = frames.dedupe(frames.trades_frame(records))
    assert df.loc[0, "source"] == "STREAM"


def test_the_same_trade_id_on_two_venues_is_two_trades():
    # trade_id is only unique within a venue, which is why the natural key
    # carries venue and venue_symbol as well.
    records = [
        record(venue="binance", venue_symbol="BTCUSDT", trade_id="1"),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="1"),
    ]
    assert len(frames.dedupe(frames.trades_frame(records))) == 2


def test_bars_compute_ohlcv_and_vwap():
    records = [
        record(trade_id="1", price="100", size="1", offset_us=0),
        record(trade_id="2", price="102", size="3", offset_us=1_000_000),
    ]
    bars = frames.bars(frames.trades_frame(records), freq="1min")
    assert len(bars) == 1
    row = bars.iloc[0]
    assert row["open"] == pytest.approx(100.0)
    assert row["high"] == pytest.approx(102.0)
    assert row["low"] == pytest.approx(100.0)
    assert row["close"] == pytest.approx(102.0)
    assert row["volume"] == pytest.approx(4.0)
    assert row["trades"] == 2
    # (100*1 + 102*3) / 4
    assert row["vwap"] == pytest.approx(101.5)


def test_vwap_on_a_flat_bar_lands_within_a_float_ulp():
    # Every trade at one price: sum(p*s)/sum(s) is p only up to float64
    # rounding, so a zero-tolerance low <= vwap <= high check reports a
    # violation that is not one. Observed on live data (6 of 97 bars).
    records = [
        record(trade_id=str(i), price="6.517", size="1.1", offset_us=i * 1_000) for i in range(5)
    ]
    bars = frames.bars(frames.trades_frame(records), freq="1min")
    row = bars.iloc[0]
    assert row["low"] == row["high"]
    assert row["vwap"] == pytest.approx(6.517, abs=1e-12)


def test_bars_split_on_the_frequency_boundary():
    records = [
        record(trade_id="1", price="100", size="1", offset_us=0),
        record(trade_id="2", price="200", size="1", offset_us=120_000_000),
    ]
    bars = frames.bars(frames.trades_frame(records), freq="1min")
    assert len(bars) == 2


def test_bars_default_to_a_consolidated_tape_across_venues():
    records = [
        record(venue="binance", venue_symbol="BTCUSDT", trade_id="1", price="100", size="1"),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="2", price="110", size="1"),
    ]
    bars = frames.bars(frames.trades_frame(records), freq="1min")
    assert len(bars) == 1
    assert bars.iloc[0]["vwap"] == pytest.approx(105.0)


def test_bars_can_split_per_venue():
    records = [
        record(venue="binance", venue_symbol="BTCUSDT", trade_id="1", price="100", size="1"),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="2", price="110", size="1"),
    ]
    bars = frames.bars(frames.trades_frame(records), freq="1min", by=["venue", "instrument_id"])
    assert len(bars) == 2
    assert set(bars["venue"]) == {"binance", "coinbase"}


def test_bars_on_an_empty_frame_return_the_right_columns():
    bars = frames.bars(frames.trades_frame([]), freq="1min")
    assert bars.empty
    assert {"open", "high", "low", "close", "volume", "vwap"} <= set(bars.columns)


def test_venue_comparison_reports_the_spread():
    records = [
        record(venue="binance", venue_symbol="BTCUSDT", trade_id="1", price="100", size="1"),
        record(venue="coinbase", venue_symbol="BTC-USD", trade_id="2", price="101", size="1"),
    ]
    wide = frames.venue_comparison(frames.trades_frame(records), freq="1min")
    assert "spread" in wide.columns
    assert abs(wide.iloc[0]["spread"]) == pytest.approx(1.0)


def test_frame_keeps_dataclass_properties():
    # PartitionInfo.messages is a property; asdict() alone would drop it, and
    # it is the column you actually want in the health table.
    infos = [PartitionInfo(topic="md.trades.v1", partition=0, low=10, high=60)]
    df = frames.frame(infos)
    assert df.loc[0, "messages"] == 50


def test_frame_rejects_something_it_cannot_shape():
    with pytest.raises(TypeError):
        frames.frame(["not a record"])
