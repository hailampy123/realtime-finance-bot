"""Binance perpetual context, built from real API payloads.

Every fixture under fixtures/ is an unmodified response from fapi.binance.com,
captured while writing these tests. The four ratio endpoints return their own
5-minute grid timestamps which need not equal the poll instant, and that gap is
exactly what these tests pin.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from awsnative.enrichment.perp import (
    GRID_MINUTES,
    PerpContext,
    RatioObservation,
    build_rows,
    floor_to_grid_us,
    index_by_symbol,
)

FIXTURES = Path(__file__).parent / "fixtures"
BTC = ("BTC-USD", "BTCUSDT")
POLL_TS_US = 1_787_028_300_000_000  # 2026-08-18T05:25:00Z, exactly on the grid


def load(name: str) -> object:
    return json.loads(FIXTURES.joinpath(name).read_text())


def ratios_for(symbol: str) -> dict[str, list[dict[str, str]]]:
    return {
        "toptrader_accounts": load("top_long_short_account_ratio.json"),  # type: ignore[dict-item]
        "toptrader_positions": load("top_long_short_position_ratio.json"),  # type: ignore[dict-item]
        "global_accounts": load("global_long_short_account_ratio.json"),  # type: ignore[dict-item]
        "taker_volume": load("takerlongshort_ratio.json"),  # type: ignore[dict-item]
    }


def one_row(poll_ts_us: int = POLL_TS_US) -> PerpContext:
    return build_rows(
        pairs=[BTC],
        premium_index=load("premium_index.json"),  # type: ignore[arg-type]
        open_interest={"BTCUSDT": load("open_interest.json")},  # type: ignore[dict-item]
        ratios={"BTCUSDT": ratios_for("BTCUSDT")},
        poll_ts_us=poll_ts_us,
    )[0]


class TestFloorToGrid:
    def test_an_instant_on_the_grid_is_unchanged(self) -> None:
        assert floor_to_grid_us(POLL_TS_US) == POLL_TS_US

    def test_an_instant_mid_interval_floors_down(self) -> None:
        assert floor_to_grid_us(POLL_TS_US + 299_999_999) == POLL_TS_US

    def test_the_next_interval_starts_a_new_grid_point(self) -> None:
        step = GRID_MINUTES * 60 * 1_000_000
        assert floor_to_grid_us(POLL_TS_US + step) == POLL_TS_US + step

    def test_flooring_is_idempotent(self) -> None:
        once = floor_to_grid_us(POLL_TS_US + 123_456)
        assert floor_to_grid_us(once) == once


class TestIndexBySymbol:
    def test_it_keeps_only_the_symbols_asked_for(self) -> None:
        # premiumIndex returns 874 symbols in one response. Keeping all of them
        # would put 866 instruments nobody trades into Bronze on every poll.
        indexed = index_by_symbol(load("premium_index.json"), {"BTCUSDT"})  # type: ignore[arg-type]
        assert set(indexed) == {"BTCUSDT"}

    def test_it_keeps_every_universe_symbol_present_in_the_payload(self) -> None:
        wanted = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"}
        assert set(index_by_symbol(load("premium_index.json"), wanted)) == wanted  # type: ignore[arg-type]

    def test_a_symbol_absent_from_the_payload_is_simply_absent(self) -> None:
        indexed = index_by_symbol(load("premium_index.json"), {"NOTREALUSDT"})  # type: ignore[arg-type]
        assert indexed == {}


class TestBuildRows:
    def test_it_carries_the_funding_fields_verbatim(self) -> None:
        row = one_row()
        assert row.instrument_id == "BTC-USD"
        assert row.venue_symbol == "BTCUSDT"
        assert row.last_funding_rate == "0.00003686"
        assert row.interest_rate == "0.00010000"
        assert row.mark_price == "64182.44374813"
        assert row.index_price == "64216.74913043"

    def test_decimal_fields_are_never_parsed_to_float(self) -> None:
        # Same discipline as the archive parsers: "0.00003686" through a float and
        # back is a different string, and for a funding rate that difference
        # compounds across every bar that reads it.
        row = one_row()
        assert isinstance(row.last_funding_rate, str)
        assert isinstance(row.open_interest, str)

    def test_binance_millisecond_timestamps_become_microseconds(self) -> None:
        # The REST API answers in milliseconds; every other instant in this
        # project is microseconds. Converting at the boundary means nothing
        # downstream has to remember which API a column came from.
        row = one_row()
        assert row.next_funding_time_us == 1_787_040_000_000 * 1000
        assert row.premium_index_ts_us == 1_787_028_275_001 * 1000

    def test_each_ratio_family_keeps_its_own_source_timestamp(self) -> None:
        # Spec §7.1: the four ratio endpoints return their own 5-minute grid
        # timestamps, which need not equal the poll instant. Flattening them to
        # one timestamp would destroy the evidence X1 exists to check.
        row = one_row()
        for family in (row.toptrader_accounts, row.toptrader_positions, row.global_accounts):
            assert family.source_ts_us > 0
            assert family.source_ts_us % 1000 == 0

    def test_the_ratio_components_are_carried_not_just_the_ratio(self) -> None:
        # Spec §4.2: the live endpoints return the components the ratio was built
        # from; the archive returns only the ratio. Storing components is what
        # makes a live-row rollup valid at all.
        row = one_row()
        assert row.toptrader_accounts.long_component != ""
        assert row.toptrader_accounts.short_component != ""
        assert row.taker_volume.long_component != ""
        assert row.taker_volume.short_component != ""

    def test_taker_volume_maps_buy_and_sell_onto_long_and_short(self) -> None:
        taker = json.loads(FIXTURES.joinpath("takerlongshort_ratio.json").read_text())[-1]
        row = one_row()
        assert row.taker_volume.ratio == taker["buySellRatio"]
        assert row.taker_volume.long_component == taker["buyVol"]
        assert row.taker_volume.short_component == taker["sellVol"]

    def test_the_snapshot_is_the_grid_point_the_poll_belongs_to(self) -> None:
        row = one_row(POLL_TS_US + 137_000_000)
        assert row.snapshot_ts_us == POLL_TS_US
        assert row.poll_ts_us == POLL_TS_US + 137_000_000

    def test_it_takes_the_latest_ratio_row_when_the_endpoint_returns_several(self) -> None:
        # limit=2 returns oldest first. Taking [0] would record a reading five
        # minutes stale on every poll, which is invisible in the data and wrong.
        payload = json.loads(FIXTURES.joinpath("global_long_short_account_ratio.json").read_text())
        assert len(payload) == 2
        assert one_row().global_accounts.source_ts_us == payload[-1]["timestamp"] * 1000


class TestMissingData:
    def test_an_instrument_missing_from_premium_index_is_dropped(self) -> None:
        # Without a mark price there is no funding context worth a row.
        rows = build_rows(
            pairs=[("FAKE-USD", "FAKEUSDT")],
            premium_index=load("premium_index.json"),  # type: ignore[arg-type]
            open_interest={},
            ratios={},
            poll_ts_us=POLL_TS_US,
        )
        assert rows == []

    def test_a_missing_ratio_family_yields_empty_strings_not_zeros(self) -> None:
        # Empty means "the endpoint did not answer". Zero is a reading, and a
        # long/short ratio of zero would say every account is short.
        rows = build_rows(
            pairs=[BTC],
            premium_index=load("premium_index.json"),  # type: ignore[arg-type]
            open_interest={},
            ratios={},
            poll_ts_us=POLL_TS_US,
        )
        assert len(rows) == 1
        assert rows[0].open_interest == ""
        assert rows[0].global_accounts == RatioObservation.absent()

    def test_funding_survives_when_only_the_ratios_are_missing(self) -> None:
        # Partial data is still worth recording. Dropping the row would lose the
        # funding rate because a different endpoint timed out.
        rows = build_rows(
            pairs=[BTC],
            premium_index=load("premium_index.json"),  # type: ignore[arg-type]
            open_interest={},
            ratios={},
            poll_ts_us=POLL_TS_US,
        )
        assert rows[0].last_funding_rate == "0.00003686"


class TestJsonShape:
    def test_every_value_flattens_to_a_string(self) -> None:
        # Bronze is read by Athena as text; a nested object or a bare int would
        # need a struct column and a cast that the merge does not want.
        payload = one_row().to_json()
        assert all(isinstance(v, str) for v in payload.values())

    def test_the_ratio_families_flatten_with_stable_prefixes(self) -> None:
        payload = one_row().to_json()
        for family in (
            "toptrader_accounts",
            "toptrader_positions",
            "global_accounts",
            "taker_volume",
        ):
            assert f"{family}_ratio" in payload
            assert f"{family}_long" in payload
            assert f"{family}_short" in payload
            assert f"{family}_ts_us" in payload

    def test_all_eight_universe_instruments_build(self) -> None:
        pairs = [
            ("BTC-USD", "BTCUSDT"),
            ("ETH-USD", "ETHUSDT"),
            ("SOL-USD", "SOLUSDT"),
            ("XRP-USD", "XRPUSDT"),
            ("ADA-USD", "ADAUSDT"),
            ("LINK-USD", "LINKUSDT"),
            ("AVAX-USD", "AVAXUSDT"),
            ("DOGE-USD", "DOGEUSDT"),
        ]
        rows = build_rows(
            pairs=pairs,
            premium_index=load("premium_index.json"),  # type: ignore[arg-type]
            open_interest={},
            ratios={},
            poll_ts_us=POLL_TS_US,
        )
        assert len(rows) == 8
        assert {r.instrument_id for r in rows} == {p[0] for p in pairs}


@pytest.mark.parametrize("bad", [-1, 0])
def test_a_non_positive_poll_instant_is_rejected(bad: int) -> None:
    with pytest.raises(ValueError, match="poll_ts_us"):
        floor_to_grid_us(bad)
