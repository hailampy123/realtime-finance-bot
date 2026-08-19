"""Normalised rows to staging CSV bytes.

Staging is gzipped CSV, not Parquet, and that is a deliberate narrowing of spec
§6.2 -- see the module docstring in awsnative/backfill/staging.py. These tests
pin the column order, because the Glue table declares the same order and nothing
at runtime would notice if the two drifted: Athena would hand every value to the
wrong column and the types mostly still fit.
"""

from __future__ import annotations

import gzip
from pathlib import Path

from awsnative.backfill.parsers import parse_agg_trades, parse_klines
from awsnative.backfill.staging import (
    KLINE_COLUMNS,
    TRADE_COLUMNS,
    klines_csv_gz,
    trades_csv_gz,
)

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> list[str]:
    return FIXTURES.joinpath(name).read_text().splitlines()


def rows_of(blob: bytes) -> list[list[str]]:
    text = gzip.decompress(blob).decode()
    return [line.split(",") for line in text.splitlines()]


class TestKlinesCsvGz:
    def test_it_writes_one_row_per_bar_with_no_header(self) -> None:
        # No header: the Glue table declares the columns, and a header row would
        # be read as data by Athena unless skip.header.line.count is set on the
        # table -- one more thing that can silently disagree.
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        rows = rows_of(klines_csv_gz("BTC-USD", bars))
        assert len(rows) == 3
        assert all(len(r) == len(KLINE_COLUMNS) for r in rows)

    def test_the_instrument_id_is_added_because_the_archive_does_not_carry_it(self) -> None:
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        rows = rows_of(klines_csv_gz("BTC-USD", bars))
        assert KLINE_COLUMNS[0] == "instrument_id"
        assert {r[0] for r in rows} == {"BTC-USD"}

    def test_column_order_matches_the_declared_schema(self) -> None:
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        row = dict(zip(KLINE_COLUMNS, rows_of(klines_csv_gz("BTC-USD", bars))[0], strict=True))
        assert row["open_time_us"] == "1749945600000000"
        assert row["close_time_us"] == "1749945659999999"
        assert row["open"] == "105414.63000000"
        assert row["high"] == "105425.93000000"
        assert row["low"] == "105414.63000000"
        assert row["close"] == "105425.92000000"
        assert row["volume"] == "2.03644000"
        assert row["quote_volume"] == "214684.76776190"
        assert row["trade_count"] == "571"
        assert row["taker_buy_base_volume"] == "0.97297000"

    def test_decimal_text_is_never_reformatted(self) -> None:
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        blob = gzip.decompress(klines_csv_gz("BTC-USD", bars)).decode()
        assert "105414.63000000" in blob
        assert "105414.63," not in blob

    def test_an_empty_bar_list_produces_an_empty_blob(self) -> None:
        assert gzip.decompress(klines_csv_gz("BTC-USD", [])) == b""


class TestTradesCsvGz:
    def test_it_writes_one_row_per_trade_with_the_derived_side(self) -> None:
        trades = list(parse_agg_trades(fixture("agg_trades_millis.csv")))
        rows = rows_of(trades_csv_gz("BTC-USD", "BTCUSDT", trades))
        side_index = TRADE_COLUMNS.index("side")
        assert [r[side_index] for r in rows] == ["SELL", "SELL", "BUY"]

    def test_column_order_matches_the_declared_schema(self) -> None:
        trades = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        row = dict(
            zip(TRADE_COLUMNS, rows_of(trades_csv_gz("BTC-USD", "BTCUSDT", trades))[0], strict=True)
        )
        assert row["instrument_id"] == "BTC-USD"
        assert row["venue"] == "binance"
        assert row["agg_trade_id"] == "4033624493"
        assert row["price"] == "63043.56000000"
        assert row["quantity"] == "0.00007000"
        assert row["first_trade_id"] == "6575249309"
        assert row["last_trade_id"] == "6575249309"
        assert row["event_ts_us"] == "1786752000286442"
        assert row["side"] == "BUY"

    def test_the_venue_is_binance_because_only_binance_has_an_archive(self) -> None:
        trades = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        venue_index = TRADE_COLUMNS.index("venue")
        assert {r[venue_index] for r in rows_of(trades_csv_gz("BTC-USD", "BTCUSDT", trades))} == {
            "binance"
        }


class TestCompression:
    def test_the_blob_is_valid_gzip_and_smaller_than_the_plain_text(self) -> None:
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        blob = klines_csv_gz("BTC-USD", bars)
        assert blob[:2] == b"\x1f\x8b"
        assert len(blob) < len(gzip.decompress(blob))

    def test_output_is_byte_identical_across_two_calls(self) -> None:
        # mtime in the gzip header would otherwise make every write differ, which
        # turns a re-run into a new object and breaks any content comparison.
        bars = list(parse_klines(fixture("klines_1m_micros.csv")))
        assert klines_csv_gz("BTC-USD", bars) == klines_csv_gz("BTC-USD", bars)
