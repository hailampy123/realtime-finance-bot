"""The seeder's pure parts: the work list format and the resumability query."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from awsnative.backfill.manifest import WorkItem, plan
from awsnative.backfill.seed import (
    done_keys,
    done_keys_sql,
    instrument_pairs,
    items_key,
    to_jsonl,
)
from awsnative.backfill.tiers import Tier, files_for_window

TODAY = date(2026, 8, 17)
UNIVERSE = Path("config/universe.yaml")


def items() -> list[WorkItem]:
    files = files_for_window(
        [("BTC-USD", "BTCUSDT")], Tier.DEEP, date(2025, 6, 1), date(2025, 7, 31), today=TODAY
    )
    return plan(files, already_done=frozenset())


class TestToJsonl:
    def test_one_json_object_per_line(self) -> None:
        lines = to_jsonl(items()).decode().splitlines()
        assert len(lines) == 2
        assert [json.loads(line)["period"] for line in lines] == ["2025-06", "2025-07"]

    def test_each_line_round_trips_to_the_work_item(self) -> None:
        planned = items()
        parsed = [
            WorkItem.from_json(json.loads(line)) for line in to_jsonl(planned).decode().splitlines()
        ]
        assert parsed == planned

    def test_it_ends_with_a_newline_so_the_last_line_is_readable(self) -> None:
        # A JSON Lines reader that splits on newline drops a trailing partial
        # line, which would silently skip the last file in every plan.
        assert to_jsonl(items()).endswith(b"\n")

    def test_an_empty_plan_is_an_empty_body(self) -> None:
        assert to_jsonl([]) == b""


class TestDoneKeys:
    def test_it_collects_archive_keys(self) -> None:
        rows = [{"archive_key": "a"}, {"archive_key": "b"}]
        assert done_keys(rows) == frozenset({"a", "b"})

    def test_it_ignores_blank_and_absent_keys(self) -> None:
        # Athena returns an empty string for a NULL cell, and a header-only result
        # set yields no rows at all. Neither is a key to skip.
        assert done_keys([{"archive_key": ""}, {}]) == frozenset()


class TestDoneKeysSql:
    def test_it_filters_to_done_only(self) -> None:
        sql = done_keys_sql("fdai_native", Tier.DEEP)
        assert "status = 'DONE'" in sql
        assert "tier = 'DEEP'" in sql
        assert "fdai_native.backfill_manifest" in sql

    def test_failed_and_skipped_are_not_treated_as_done(self) -> None:
        # A FAILED row must be retried. A SKIPPED_NO_DATA row is cheap to
        # re-check and the file may have been published since.
        sql = done_keys_sql("fdai_native", Tier.HOT)
        assert "FAILED" not in sql
        assert "SKIPPED_NO_DATA" not in sql


class TestItemsKey:
    def test_one_object_per_tier(self) -> None:
        assert items_key(Tier.DEEP) == "_backfill/items/deep.jsonl"
        assert items_key(Tier.HOT) == "_backfill/items/hot.jsonl"


class TestInstrumentPairs:
    def test_it_reads_the_real_universe(self) -> None:
        pairs = instrument_pairs(UNIVERSE)
        assert ("BTC-USD", "BTCUSDT") in pairs
        assert len(pairs) == 8

    def test_every_pair_is_canonical_id_then_venue_symbol(self) -> None:
        for instrument_id, venue_symbol in instrument_pairs(UNIVERSE):
            assert instrument_id.endswith("-USD")
            assert venue_symbol.endswith("USDT")
