"""Golden-file tests for the two archive parsers.

Every fixture under fixtures/ is unmodified bytes from a real
`data.binance.vision` file. That matters: a hand-written fixture encodes what we
believe the format is, and both traps this module guards are cases where the
belief was wrong.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from awsnative.backfill.epoch import TimestampUnitError
from awsnative.backfill.parsers import parse_agg_trades, parse_klines
from ingest.core.models import Side

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> list[str]:
    return FIXTURES.joinpath(name).read_text().splitlines()


class TestParseAggTrades:
    def test_is_buyer_maker_true_means_the_aggressor_sold(self) -> None:
        # THE TRAP. isBuyerMaker = True means the BUYER was the maker, so the
        # taker -- the side that crossed the spread -- was the SELLER.
        # Reading it the other way flips every flow-imbalance feature and the
        # data stays well-formed, so no downstream check fires.
        rows = list(parse_agg_trades(fixture("agg_trades_millis.csv")))
        assert [r.side for r in rows] == [Side.SELL, Side.SELL, Side.BUY]

    def test_the_microsecond_era_parses_too(self) -> None:
        rows = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        assert [r.side for r in rows] == [Side.BUY, Side.BUY, Side.BUY]

    def test_timestamps_normalise_to_microseconds_in_both_eras(self) -> None:
        millis = list(parse_agg_trades(fixture("agg_trades_millis.csv")))
        micros = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        assert millis[0].event_ts_us == 1_686_787_200_000_000
        assert micros[0].event_ts_us == 1_786_752_000_286_442

    def test_decimal_fields_keep_their_exact_source_text(self) -> None:
        # Never float. "0.00007000" round-tripped through float and reformatted
        # is a different string, and for a price that difference is the bug.
        rows = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        assert rows[0].price == "63043.56000000"
        assert rows[0].quantity == "0.00007000"

    def test_identity_fields_survive(self) -> None:
        rows = list(parse_agg_trades(fixture("agg_trades_micros.csv")))
        assert rows[0].agg_trade_id == 4_033_624_493
        assert rows[1].first_trade_id == 6_575_249_310
        assert rows[1].last_trade_id == 6_575_249_311

    def test_a_blank_line_is_skipped_not_parsed(self) -> None:
        rows = list(parse_agg_trades(["", *fixture("agg_trades_micros.csv"), ""]))
        assert len(rows) == 3

    def test_a_row_with_the_wrong_column_count_raises(self) -> None:
        with pytest.raises(ValueError, match="8 columns"):
            list(parse_agg_trades(["1,2,3"]))

    def test_an_unparseable_boolean_raises_rather_than_defaulting(self) -> None:
        # Defaulting an unknown isBuyerMaker to either side would invent a
        # direction. Binance writes Python-style True/False; anything else means
        # the format changed and the parser must be updated, not guess.
        bad = "1,2.0,3.0,4,5,1786752000286442,yes,True"
        with pytest.raises(ValueError, match="is_buyer_maker"):
            list(parse_agg_trades([bad]))


class TestParseKlines:
    def test_the_millisecond_era_normalises_to_microseconds(self) -> None:
        rows = list(parse_klines(fixture("klines_1m_millis.csv")))
        assert rows[0].open_time_us == 1_686_787_200_000_000
        assert rows[0].close_time_us == 1_686_787_259_999_000

    def test_the_microsecond_era_passes_through(self) -> None:
        rows = list(parse_klines(fixture("klines_1m_micros.csv")))
        assert rows[0].open_time_us == 1_749_945_600_000_000
        assert rows[0].close_time_us == 1_749_945_659_999_999

    def test_the_additive_components_gold_needs_are_all_present(self) -> None:
        # These six map onto gold_bars_1m's additive columns. quote_volume is
        # notional and taker_buy_base_volume is buy_vol; sell_vol is derived in
        # SQL as volume - buy_vol, so the parser does no arithmetic at all.
        rows = list(parse_klines(fixture("klines_1m_micros.csv")))
        row = rows[0]
        assert row.open == "105414.63000000"
        assert row.high == "105425.93000000"
        assert row.low == "105414.63000000"
        assert row.close == "105425.92000000"
        assert row.volume == "2.03644000"
        assert row.quote_volume == "214684.76776190"
        assert row.taker_buy_base_volume == "0.97297000"
        assert row.trade_count == 571

    def test_all_three_rows_parse(self) -> None:
        assert len(list(parse_klines(fixture("klines_1m_micros.csv")))) == 3

    def test_a_row_with_the_wrong_column_count_raises(self) -> None:
        with pytest.raises(ValueError, match="12 columns"):
            list(parse_klines(["1,2,3"]))

    def test_a_file_mixing_the_two_eras_raises(self) -> None:
        mixed = [*fixture("klines_1m_millis.csv"), *fixture("klines_1m_micros.csv")]
        with pytest.raises(TimestampUnitError, match="disagrees"):
            list(parse_klines(mixed))

    def test_open_and_close_time_are_normalised_by_the_same_unit(self) -> None:
        # close_time in the millisecond era ends 59999, which is 59999000 in
        # microseconds -- NOT 59999999. Scaling it by a unit detected from
        # open_time only is what keeps the two consistent within a row.
        rows = list(parse_klines(fixture("klines_1m_millis.csv")))
        assert rows[0].close_time_us - rows[0].open_time_us == 59_999_000
