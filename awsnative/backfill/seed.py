"""Plan one backfill run and write the work list Distributed Map will read.

    python -m awsnative.backfill.seed --tier DEEP --start 2024-08-17 --end 2026-08-16 \
        --bucket fdai-native-lake-123 --database fdai_native --workgroup fdai-native

WHY THE PLAN IS A FILE AND NOT A QUERY. Step Functions' Distributed Map reads its
items from S3, not from Athena. Rather than fight that, the resumability check
happens here, once, as a single query for the DONE keys -- so the map's input is
already only the work that remains.

WHY THIS IS NOT A LAMBDA. Seeding is an operator action taken at a chosen moment
with a chosen window, and its output is worth reading before spending an hour of
downloads. `make backfill-plan-aws` prints the counts; nothing runs until the
state machine is started.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from awsnative.backfill.manifest import STAGING_PREFIXES, WorkItem, plan
from awsnative.backfill.tiers import ARCHIVE_VENUE, Tier, files_for_window
from ingest.core.instruments import InstrumentMap

ITEMS_PREFIX = "_backfill/items"


def items_key(tier: Tier) -> str:
    """Where the work list for `tier` lives. One object per tier, overwritten."""
    return f"{ITEMS_PREFIX}/{tier.value.lower()}.jsonl"


def to_jsonl(items: list[WorkItem]) -> bytes:
    """One compact JSON object per line, which is what Distributed Map expects."""
    return "".join(
        f"{json.dumps(item.to_json(), separators=(',', ':'))}\n" for item in items
    ).encode()


def done_keys(rows: list[dict[str, str]]) -> frozenset[str]:
    """Archive keys already recorded DONE, from the manifest query's result rows."""
    return frozenset(row["archive_key"] for row in rows if row.get("archive_key"))


def done_keys_sql(database: str, tier: Tier) -> str:
    """The one query that makes a run resumable.

    Restricted to DONE: a FAILED row must be retried, and a SKIPPED_NO_DATA row is
    cheap to re-check and might have been published since.
    """
    return (
        f"SELECT archive_key FROM {database}.backfill_manifest "
        f"WHERE tier = '{tier.value}' AND status = 'DONE'"
    )


def instrument_pairs(universe_path: Path) -> list[tuple[str, str]]:
    """(instrument_id, venue_symbol) for every Binance instrument in the universe."""
    universe = InstrumentMap.from_yaml(universe_path)
    return [
        (universe.canonical(ARCHIVE_VENUE, symbol), symbol)
        for symbol in universe.symbols_for(ARCHIVE_VENUE)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument("--start", required=True, help="first day to cover, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last day to cover, inclusive")
    parser.add_argument("--bucket", required=True, help="lake bucket name, no s3:// prefix")
    parser.add_argument("--database", required=True)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--universe", type=Path, default=Path("config/universe.yaml"))
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="re-fetch everything: skip the DONE-key query and plan the whole window",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the plan and exit without reading the manifest or writing to S3",
    )
    args = parser.parse_args(argv)

    tier = Tier(args.tier)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if start > end:
        print(f"--start {start} is after --end {end}", file=sys.stderr)
        return 2

    today = datetime.now(tz=UTC).date()
    files = files_for_window(instrument_pairs(args.universe), tier, start, end, today=today)

    if args.dry_run:
        items = plan(files, already_done=frozenset())
        print(f"{len(items)} files would be planned for {tier.value} over {start}..{end}")
        for item in items[:10]:
            print(f"  {item.archive_key}")
        if len(items) > 10:
            print(f"  ... and {len(items) - 10} more")
        return 0

    already = frozenset[str]()
    if not args.fresh:
        from awsnative.athena import AthenaRunner

        runner = AthenaRunner(database=args.database, workgroup=args.workgroup)
        outcome = runner.execute(done_keys_sql(args.database, tier))
        already = done_keys(runner.fetch_rows(outcome.query_execution_id, max_rows=1000))
        print(f"{len(already)} files already DONE")

    items = plan(files, already_done=already)
    if not items:
        print("nothing to do: every file in the window is already DONE")
        return 0

    import boto3

    key = items_key(tier)
    boto3.client("s3").put_object(Bucket=args.bucket, Key=key, Body=to_jsonl(items))

    print(f"{len(items)} files planned -> s3://{args.bucket}/{key}")
    print(f"staging prefix: s3://{args.bucket}/{STAGING_PREFIXES[tier]}/")
    print("start the state machine with: make backfill-aws TIER=" + tier.value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
