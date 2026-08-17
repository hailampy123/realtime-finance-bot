"""Which archive files a window needs, and where they live.

Every URL asserted here was fetched successfully while writing these tests. The
monthly/daily split is not a micro-optimisation: a two-year deep tier is ~5,840
daily files or ~192 monthly ones, and that is the difference between a backfill
measured in tens of minutes and one measured in under a minute.
"""

from __future__ import annotations

from datetime import date

from awsnative.backfill.tiers import (
    ARCHIVE_BASE_URL,
    ArchiveFile,
    Granularity,
    Tier,
    files_for_window,
)

BTC = ("BTC-USD", "BTCUSDT")
ETH = ("ETH-USD", "ETHUSDT")


def keys(files: list[ArchiveFile]) -> list[str]:
    return [f.key for f in files]


class TestUrlShapes:
    def test_deep_monthly_matches_the_real_archive_layout(self) -> None:
        files = files_for_window(
            [BTC], Tier.DEEP, date(2025, 6, 1), date(2025, 6, 30), today=date(2026, 8, 17)
        )
        assert keys(files) == ["data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-06.zip"]
        assert files[0].url == f"{ARCHIVE_BASE_URL}/{files[0].key}"
        assert files[0].checksum_url == f"{files[0].url}.CHECKSUM"

    def test_deep_daily_matches_the_real_archive_layout(self) -> None:
        files = files_for_window(
            [BTC], Tier.DEEP, date(2026, 8, 15), date(2026, 8, 15), today=date(2026, 8, 17)
        )
        assert keys(files) == ["data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-08-15.zip"]

    def test_hot_daily_matches_the_real_archive_layout(self) -> None:
        files = files_for_window(
            [BTC], Tier.HOT, date(2026, 8, 15), date(2026, 8, 15), today=date(2026, 8, 17)
        )
        assert keys(files) == ["data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-08-15.zip"]

    def test_hot_monthly_matches_the_real_archive_layout(self) -> None:
        files = files_for_window(
            [BTC], Tier.HOT, date(2025, 6, 1), date(2025, 6, 30), today=date(2026, 8, 17)
        )
        assert keys(files) == ["data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-06.zip"]


class TestGranularityChoice:
    def test_a_fully_covered_published_month_uses_one_monthly_file(self) -> None:
        files = files_for_window(
            [BTC], Tier.DEEP, date(2025, 3, 1), date(2025, 3, 31), today=date(2026, 8, 17)
        )
        assert len(files) == 1
        assert files[0].granularity is Granularity.MONTHLY

    def test_a_partially_covered_month_uses_daily_files(self) -> None:
        # A monthly file covers the whole month. Using it for a window that
        # covers part of the month would merge bars from outside the window,
        # which silently changes what the window means.
        files = files_for_window(
            [BTC], Tier.DEEP, date(2025, 3, 10), date(2025, 3, 12), today=date(2026, 8, 17)
        )
        assert [f.granularity for f in files] == [Granularity.DAILY] * 3
        assert [f.period for f in files] == ["2025-03-10", "2025-03-11", "2025-03-12"]

    def test_the_current_month_uses_daily_files(self) -> None:
        # Confirmed against the real archive: the monthly file for an incomplete
        # month returns HTTP 404.
        files = files_for_window(
            [BTC], Tier.DEEP, date(2026, 8, 1), date(2026, 8, 16), today=date(2026, 8, 17)
        )
        assert {f.granularity for f in files} == {Granularity.DAILY}
        assert len(files) == 16

    def test_a_month_that_ended_inside_the_publication_lag_uses_daily_files(self) -> None:
        # Binance publishes a monthly file "at the first monday of the month",
        # so July's monthly file is not reliably there on 2 August. Seven days
        # of slack means the planner never asks for a file that does not exist
        # yet, and no runtime fallback is needed.
        files = files_for_window(
            [BTC], Tier.DEEP, date(2026, 7, 1), date(2026, 7, 31), today=date(2026, 8, 2)
        )
        assert {f.granularity for f in files} == {Granularity.DAILY}
        assert len(files) == 31

    def test_the_same_month_becomes_monthly_once_the_lag_has_passed(self) -> None:
        files = files_for_window(
            [BTC], Tier.DEEP, date(2026, 7, 1), date(2026, 7, 31), today=date(2026, 8, 8)
        )
        assert [f.granularity for f in files] == [Granularity.MONTHLY]


class TestWindowSpanning:
    def test_a_window_spanning_months_mixes_granularities_correctly(self) -> None:
        # June is fully covered and published -> monthly.
        # The first half of July is partial -> daily.
        files = files_for_window(
            [BTC], Tier.DEEP, date(2025, 6, 1), date(2025, 7, 3), today=date(2026, 8, 17)
        )
        assert [(f.granularity, f.period) for f in files] == [
            (Granularity.MONTHLY, "2025-06"),
            (Granularity.DAILY, "2025-07-01"),
            (Granularity.DAILY, "2025-07-02"),
            (Granularity.DAILY, "2025-07-03"),
        ]

    def test_two_years_of_deep_tier_is_hundreds_of_files_not_thousands(self) -> None:
        files = files_for_window(
            [BTC, ETH], Tier.DEEP, date(2024, 8, 17), date(2026, 8, 16), today=date(2026, 8, 17)
        )
        # 23 whole months + a partial August 2024 + a partial August 2026, per
        # instrument. Daily granularity for the same window would be ~1,460.
        assert len(files) < 200
        assert sum(1 for f in files if f.granularity is Granularity.MONTHLY) == 46

    def test_output_is_chronological_per_instrument_and_deterministic(self) -> None:
        args = ([ETH, BTC], Tier.DEEP, date(2025, 1, 1), date(2025, 3, 31))
        first = files_for_window(*args, today=date(2026, 8, 17))
        second = files_for_window(*args, today=date(2026, 8, 17))
        assert keys(first) == keys(second)
        # Grouped by instrument regardless of input order, then chronological.
        assert [(f.instrument_id, f.period) for f in first] == [
            ("BTC-USD", "2025-01"),
            ("BTC-USD", "2025-02"),
            ("BTC-USD", "2025-03"),
            ("ETH-USD", "2025-01"),
            ("ETH-USD", "2025-02"),
            ("ETH-USD", "2025-03"),
        ]

    def test_an_empty_window_yields_nothing(self) -> None:
        assert (
            files_for_window(
                [BTC], Tier.DEEP, date(2025, 3, 5), date(2025, 3, 4), today=date(2026, 8, 17)
            )
            == []
        )


class TestPartitionDate:
    def test_a_monthly_file_reports_the_first_day_of_its_month(self) -> None:
        files = files_for_window(
            [BTC], Tier.DEEP, date(2025, 6, 1), date(2025, 6, 30), today=date(2026, 8, 17)
        )
        assert files[0].dt == date(2025, 6, 1)

    def test_a_daily_file_reports_its_own_day(self) -> None:
        files = files_for_window(
            [BTC], Tier.HOT, date(2026, 8, 15), date(2026, 8, 15), today=date(2026, 8, 17)
        )
        assert files[0].dt == date(2026, 8, 15)
