"""The whole page, assembled from query results. No AWS account involved."""

from __future__ import annotations

from awsnative.dashboard.cli import build_page_from_rows, freshness_tile

FRESHNESS = [
    {"table_name": "silver_trades", "lag_seconds": "240", "row_count": "184320"},
    {"table_name": "gold_bars_1m", "lag_seconds": "300", "row_count": "11520"},
    {"table_name": "silver_perp_context", "lag_seconds": "2400", "row_count": "2304"},
    {"table_name": "silver_macro", "lag_seconds": "", "row_count": "0"},
]
COUNTS = [
    {"dt": "2026-08-16", "layer": "Bronze trades", "row_count": "412000"},
    {"dt": "2026-08-16", "layer": "Silver trades", "row_count": "398112"},
    {"dt": "2026-08-16", "layer": "Gold bars", "row_count": "11520"},
    {"dt": "2026-08-17", "layer": "Bronze trades", "row_count": "437100"},
    {"dt": "2026-08-17", "layer": "Silver trades", "row_count": "421884"},
    {"dt": "2026-08-17", "layer": "Gold bars", "row_count": "11520"},
]
QUARANTINE = [
    {"dt": "2026-08-16", "kind": "Accepted", "row_count": "398112"},
    {"dt": "2026-08-16", "kind": "Quarantined", "row_count": "37"},
    {"dt": "2026-08-17", "kind": "Accepted", "row_count": "421884"},
    {"dt": "2026-08-17", "kind": "Quarantined", "row_count": "12"},
]
PERP = [
    {
        "ts": str(1_787_000_000 + i * 300),
        "label": f"2026-08-18 0{i}:00",
        "mark_price": f"{64000 + i * 37.5:.2f}",
        "funding_rate": f"{(-1) ** i * 0.00003 + i * 1e-6:.8f}",
        "open_interest": f"{112000 + i * 210}",
    }
    for i in range(9)
]
MACRO = [
    {
        "series_id": "DGS10",
        "observation_date": "2026-08-14",
        "vintage_date": "2026-08-18",
        "value": "4.68",
    },
    {
        "series_id": "VIXCLS",
        "observation_date": "2026-08-14",
        "vintage_date": "2026-08-18",
        "value": "14.25",
    },
    {
        "series_id": "CPIAUCSL",
        "observation_date": "2026-07-01",
        "vintage_date": "2026-08-18",
        "value": "332.813",
    },
]

RESULTS = {
    "01_layer_counts.sql": COUNTS,
    "02_freshness.sql": FRESHNESS,
    "03_quarantine.sql": QUARANTINE,
    "04_perp_context.sql": PERP,
    "05_macro.sql": MACRO,
}


def page(**overrides: object) -> str:
    kwargs = {
        "results": RESULTS,
        "database": "fdai_native",
        "instrument_id": "BTC-USD",
        "lookback_days": 7,
        "generated_at": "2026-08-18 06:00",
    }
    kwargs.update(overrides)
    return build_page_from_rows(**kwargs)  # type: ignore[arg-type]


class TestFreshnessBands:
    def test_inside_the_cadence_is_good(self) -> None:
        assert "OK" in freshness_tile(FRESHNESS[0])

    def test_past_the_cadence_but_under_an_hour_is_a_warning(self) -> None:
        assert "WARN" in freshness_tile(FRESHNESS[2])

    def test_hours_behind_escalates_past_warning(self) -> None:
        # 3600s is the boundary; a tick missed is a warning, an hour gone is not.
        tile = freshness_tile({"table_name": "t", "lag_seconds": "7200", "row_count": "9"})
        assert "STALE" in tile and "hours behind" in tile

    def test_over_a_day_behind_is_critical(self) -> None:
        tile = freshness_tile({"table_name": "t", "lag_seconds": "200000", "row_count": "9"})
        assert "FAIL" in tile

    def test_a_table_with_no_rows_reads_as_never_written_not_as_stale(self) -> None:
        # Never started and fallen behind have different causes. Reporting a
        # 56-year lag for an empty table sends the reader to the wrong place.
        tile = freshness_tile(FRESHNESS[3])
        assert "no rows" in tile and "never written" in tile
        assert "FAIL" in tile

    def test_a_missing_timestamp_with_rows_is_a_warning_not_a_crash(self) -> None:
        assert "WARN" in freshness_tile({"table_name": "t", "lag_seconds": "", "row_count": "5"})


class TestPage:
    def test_it_is_one_self_contained_document(self) -> None:
        # No CDN, no bundle step, no external font: the account is wiped weekly,
        # so the page has to open from file:// with no network.
        html = page()
        assert html.startswith("<!doctype html>")
        for external in ("http://", "https://", "<script"):
            assert external not in html

    def test_it_declares_dark_mode_under_both_scopes(self) -> None:
        html = page()
        assert "prefers-color-scheme: dark" in html
        assert '[data-theme="dark"]' in html

    def test_every_section_that_has_data_appears(self) -> None:
        html = page()
        for heading in ("Freshness", "Volume and quarantine", "Perpetual context", "Macro regime"):
            assert heading in html

    def test_the_showcase_is_three_separate_panels_not_one_dual_axis_chart(self) -> None:
        # The rule this page exists to respect. Three measures with no shared
        # scale get three panels on one x-axis; two y-scales on one plot would
        # let the author pick the correlation the reader sees.
        html = page()
        for measure in ("Mark price", "Funding rate", "Open interest"):
            assert f"BTC-USD · {measure}" in html
        assert html.count('class="viz-panel"') >= 3

    def test_a_table_view_backs_the_charts(self) -> None:
        # Required, not optional: light-mode aqua fails the 3:1 contrast bar and
        # the relief for that is visible labels or a table.
        assert page().count("table view") >= 2

    def test_the_vintage_caveat_is_stated_on_the_page(self) -> None:
        assert "vintage_date, never observation_date" in page()

    def test_empty_results_still_produce_a_page(self) -> None:
        html = page(results={name: [] for name in RESULTS})
        assert html.startswith("<!doctype html>")
        assert "Data layer" in html

    def test_a_failed_query_removes_its_section_without_breaking_the_rest(self) -> None:
        partial = dict(RESULTS)
        partial["04_perp_context.sql"] = []
        html = build_page_from_rows(
            results=partial,
            database="d",
            instrument_id="BTC-USD",
            lookback_days=7,
            generated_at="now",
        )
        assert "Volume and quarantine" in html
        assert "no rows in the window" in html
