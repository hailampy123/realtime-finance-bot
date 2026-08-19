"""Work planning and outcome recording.

The manifest is what makes a backfill resumable, and `plan` is the half that
makes it cheap: it is the only place that decides not to re-fetch something.
"""

from __future__ import annotations

from datetime import date

import pytest

from awsnative.backfill.manifest import (
    Outcome,
    Status,
    WorkItem,
    plan,
    staging_key_for,
)
from awsnative.backfill.tiers import Tier, files_for_window

BTC = ("BTC-USD", "BTCUSDT")
ETH = ("ETH-USD", "ETHUSDT")
TODAY = date(2026, 8, 17)


def deep_june() -> list:
    return files_for_window([BTC], Tier.DEEP, date(2025, 6, 1), date(2025, 6, 30), today=TODAY)


class TestPlan:
    def test_it_turns_archive_files_into_work_items(self) -> None:
        items = plan(deep_june(), already_done=frozenset())
        assert len(items) == 1
        item = items[0]
        assert item.tier is Tier.DEEP
        assert item.instrument_id == "BTC-USD"
        assert item.venue_symbol == "BTCUSDT"
        assert item.url.endswith("BTCUSDT-1m-2025-06.zip")
        assert item.checksum_url.endswith(".zip.CHECKSUM")

    def test_a_done_key_is_skipped(self) -> None:
        # Spec §6.2: "A run skips DONE partitions." The checksum is what makes
        # that skip trustworthy, and it already happened when DONE was written.
        done = frozenset({deep_june()[0].key})
        assert plan(deep_june(), already_done=done) == []

    def test_only_the_done_keys_are_skipped(self) -> None:
        files = files_for_window([BTC], Tier.DEEP, date(2025, 6, 1), date(2025, 8, 31), today=TODAY)
        done = frozenset({files[0].key})
        remaining = plan(files, already_done=done)
        assert len(remaining) == len(files) - 1
        assert files[0].key not in {i.archive_key for i in remaining}

    def test_it_preserves_the_planner_ordering(self) -> None:
        files = files_for_window(
            [ETH, BTC], Tier.DEEP, date(2025, 1, 1), date(2025, 2, 28), today=TODAY
        )
        items = plan(files, already_done=frozenset())
        assert [i.instrument_id for i in items] == ["BTC-USD", "BTC-USD", "ETH-USD", "ETH-USD"]


class TestWorkItemJson:
    def test_it_round_trips_through_json(self) -> None:
        # Step Functions Distributed Map reads these as JSON Lines from S3 and
        # hands each one to the Lambda verbatim, so the dict IS the interface.
        original = plan(deep_june(), already_done=frozenset())[0]
        assert WorkItem.from_json(original.to_json()) == original

    def test_every_json_value_is_a_string(self) -> None:
        # Distributed Map's JSON Lines reader does not coerce types, and a
        # Lambda that receives an int where it expected a string fails on the
        # first f-string rather than at the boundary. Strings everywhere removes
        # the question.
        payload = plan(deep_june(), already_done=frozenset())[0].to_json()
        assert all(isinstance(v, str) for v in payload.values())

    def test_it_carries_the_staging_key_the_loader_will_write(self) -> None:
        item = plan(deep_june(), already_done=frozenset())[0]
        assert item.staging_key == staging_key_for(item)
        assert item.staging_key.startswith("archive_staging_klines/")
        assert item.staging_key.endswith(".csv.gz")


class TestStagingKey:
    def test_deep_and_hot_land_in_different_prefixes(self) -> None:
        # Two tiers, two column sets, therefore two tables. klines are bars and
        # aggTrades are trades; one Glue table cannot describe both.
        deep = plan(deep_june(), already_done=frozenset())[0]
        hot_files = files_for_window(
            [BTC], Tier.HOT, date(2026, 8, 15), date(2026, 8, 15), today=TODAY
        )
        hot = plan(hot_files, already_done=frozenset())[0]
        assert deep.staging_key.startswith("archive_staging_klines/")
        assert hot.staging_key.startswith("archive_staging_trades/")

    def test_the_key_is_unique_per_instrument_and_period(self) -> None:
        files = files_for_window(
            [BTC, ETH], Tier.DEEP, date(2025, 1, 1), date(2025, 3, 31), today=TODAY
        )
        keys = [i.staging_key for i in plan(files, already_done=frozenset())]
        assert len(set(keys)) == len(keys)


class TestOutcome:
    def test_a_successful_outcome_records_the_digest_and_the_row_count(self) -> None:
        outcome = Outcome.done(
            archive_key="data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-06.zip",
            sha256="ab" * 32,
            row_count=43_200,
        )
        assert outcome.status is Status.DONE
        assert outcome.row_count == 43_200
        assert outcome.error == ""

    def test_a_missing_file_is_skipped_not_failed(self) -> None:
        # A symbol that did not trade on a given day has no archive file. That is
        # data, not an error, and marking it FAILED would make a clean backfill
        # look broken and hide the runs that really did fail.
        outcome = Outcome.skipped_no_data(archive_key="k", reason="HTTP 404")
        assert outcome.status is Status.SKIPPED_NO_DATA
        assert outcome.row_count == 0
        assert "404" in outcome.error

    def test_a_failure_records_the_reason(self) -> None:
        outcome = Outcome.failed(archive_key="k", reason="digest mismatch")
        assert outcome.status is Status.FAILED
        assert outcome.error == "digest mismatch"

    def test_outcomes_serialise_to_flat_string_json(self) -> None:
        payload = Outcome.done(archive_key="k", sha256="ab" * 32, row_count=7).to_json()
        assert all(isinstance(v, str) for v in payload.values())
        assert payload["row_count"] == "7"

    @pytest.mark.parametrize("status", list(Status))
    def test_every_status_the_spec_names_exists(self, status: Status) -> None:
        assert status.value in {"PENDING", "RUNNING", "DONE", "FAILED", "SKIPPED_NO_DATA"}
