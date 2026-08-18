"""FRED/ALFRED parsing, and the vintage boundary that makes macro honest.

The two CPI fixtures are trimmed from real ALFRED responses and they carry a real
revision: August 2025 CPI read 323.364 in the January 2026 vintage and 323.291 in
the April 2026 vintage. A backtest standing on 2025-09-01 could only have seen
323.364. Reading today's value there is a lookahead leak, and `vintage_date` is
what closes it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from awsnative.enrichment.macro import (
    SERIES,
    SERIES_BY_ID,
    MacroObservation,
    alfred_csv_url,
    parse_alfred_csv,
)

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> str:
    return FIXTURES.joinpath(name).read_text()


def parse(name: str, series_id: str, vintage: str, **kw: object) -> list[MacroObservation]:
    return parse_alfred_csv(fixture(name), series_id=series_id, vintage_date=vintage, **kw)  # type: ignore[arg-type]


class TestSeriesSet:
    def test_the_six_series_the_design_names(self) -> None:
        assert {s.series_id for s in SERIES} == {
            "DTWEXBGS",
            "DGS2",
            "DGS10",
            "VIXCLS",
            "SP500",
            "CPIAUCSL",
        }

    def test_no_derived_spread_is_stored(self) -> None:
        # DGS10 - DGS2 is derivable. The same rule that forbids a stored vwap in
        # Gold forbids a stored 2s10s spread here: store the components.
        assert not any("T10Y2Y" in s.series_id for s in SERIES)

    def test_exactly_one_series_is_marked_revised(self) -> None:
        # CPIAUCSL earns its place by being the only one that exercises the
        # vintage machinery. If it is ever cut, cut the machinery with it.
        revised = [s.series_id for s in SERIES if s.revised]
        assert revised == ["CPIAUCSL"]

    def test_every_series_is_reachable_by_id(self) -> None:
        assert set(SERIES_BY_ID) == {s.series_id for s in SERIES}


class TestUrl:
    def test_it_asks_alfred_for_an_explicit_vintage(self) -> None:
        # Always ALFRED with a vintage, never plain FRED. That is what makes
        # vintage_date a real knowledge boundary for every series rather than a
        # proxy for "whenever we happened to pull".
        url = alfred_csv_url("DGS10", "2026-08-15")
        assert url.startswith("https://alfred.stlouisfed.org/")
        assert "id=DGS10" in url
        assert "vintage_date=2026-08-15" in url

    def test_no_api_key_appears_in_the_url(self) -> None:
        # Verified against the live endpoint: ALFRED's CSV export needs no key,
        # so the parent design's "no API key anywhere at all" survives intact.
        assert "api_key" not in alfred_csv_url("CPIAUCSL", "2026-01-15")


class TestParse:
    def test_it_reads_a_daily_series(self) -> None:
        rows = parse("dgs10_recent.csv", "DGS10", "2026-08-15")
        assert len(rows) == 6
        assert rows[0].observation_date == "2026-08-07"
        assert rows[0].value == "4.65"

    def test_every_row_carries_the_requested_vintage(self) -> None:
        rows = parse("dgs10_recent.csv", "DGS10", "2026-08-15")
        assert {r.vintage_date for r in rows} == {"2026-08-15"}

    def test_values_keep_their_exact_source_text(self) -> None:
        rows = parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15")
        assert rows[-1].value == "326.030"  # not 326.03

    def test_a_missing_observation_is_skipped_not_zeroed(self) -> None:
        # FRED writes an empty cell for an observation it has no value for.
        # October 2025 CPI is empty in both real vintages. Zero would be a
        # reading; absent is the truth.
        rows = parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15")
        assert "2025-10-01" not in {r.observation_date for r in rows}
        assert len(rows) == 4

    def test_the_since_bound_trims_older_observations(self) -> None:
        rows = parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15", since="2025-11-01")
        assert [r.observation_date for r in rows] == ["2025-11-01", "2025-12-01"]

    def test_a_header_naming_another_series_raises(self) -> None:
        # ALFRED encodes the series and vintage in the value column, as
        # CPIAUCSL_20260115. Checking it is what catches a mis-built URL that
        # returned a different series -- which would otherwise load silently.
        with pytest.raises(ValueError, match="DGS2"):
            parse("dgs10_recent.csv", "DGS2", "2026-08-15")

    @pytest.mark.parametrize("text", ["", "\n", "observation_date\n", "nonsense"])
    def test_a_malformed_response_raises(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_alfred_csv(text, series_id="DGS10", vintage_date="2026-08-15")


class TestTheRevision:
    def test_two_vintages_of_one_observation_disagree(self) -> None:
        # The whole reason the vintage column exists, asserted against real bytes.
        january = {
            r.observation_date: r.value
            for r in parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15")
        }
        april = {
            r.observation_date: r.value
            for r in parse("cpi_vintage_2026-04-15.csv", "CPIAUCSL", "2026-04-15")
        }
        assert january["2025-08-01"] == "323.364"
        assert april["2025-08-01"] == "323.291"

    def test_the_two_vintages_produce_distinct_keys(self) -> None:
        # Keyed on (series_id, observation_date, vintage_date), a revision is a
        # NEW row rather than an update -- which is what preserves the history
        # the point-in-time join depends on.
        january = parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15")
        april = parse("cpi_vintage_2026-04-15.csv", "CPIAUCSL", "2026-04-15")
        keys = {(r.series_id, r.observation_date, r.vintage_date) for r in january + april}
        assert len(keys) == len(january) + len(april)

    def test_the_recent_tail_is_where_revisions_cluster(self) -> None:
        # Every observation in these fixtures revised, and that is not a quirk of
        # the trim: they are the five most recent months, which is precisely the
        # window February's seasonal recalculation rewrites. Across the full
        # series the rate is 59 of 947, so a merge that inserted every pulled
        # vintage would store the same unchanged history daily -- which is why
        # merge_silver_macro.sql inserts only when the value actually changed.
        january = {
            r.observation_date: r.value
            for r in parse("cpi_vintage_2026-01-15.csv", "CPIAUCSL", "2026-01-15")
        }
        april = {
            r.observation_date: r.value
            for r in parse("cpi_vintage_2026-04-15.csv", "CPIAUCSL", "2026-04-15")
        }
        changed = [d for d in january if january[d] != april.get(d)]
        assert changed == sorted(january)
        # Including one that moved by a single thousandth, which a tolerance-based
        # comparison would have thrown away.
        assert (january["2025-12-01"], april["2025-12-01"]) == ("326.030", "326.031")


class TestJsonShape:
    def test_every_value_is_a_string(self) -> None:
        payload = parse("dgs10_recent.csv", "DGS10", "2026-08-15")[0].to_json()
        assert all(isinstance(v, str) for v in payload.values())

    def test_it_carries_the_four_fields_silver_keys_on(self) -> None:
        payload = parse("dgs10_recent.csv", "DGS10", "2026-08-15")[0].to_json()
        assert {"series_id", "observation_date", "vintage_date", "value"} <= set(payload)
