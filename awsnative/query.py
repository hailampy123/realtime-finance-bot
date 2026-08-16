"""Run a .sql file against Athena and print the results.

The Athena console is the better place to read a query the first time -- it
shows the execution plan and the scanned bytes without being asked. This exists
for the second time onward, when you want the same six acceptance queries after
every change and clicking through the console stops being a lesson.

Data scanned is printed for every statement deliberately. It is the number that
tells you whether partition pruning is working, and it is the number the bill is
computed from; hiding it would make the cheap thing and the expensive thing look
identical.
"""

from __future__ import annotations

import argparse
import sys

from awsnative.athena import AthenaError, AthenaRunner, split_statements


def _print_table(rows: list[dict[str, str]]) -> None:
    if not rows:
        print("    (no rows)")
        return
    headers = list(rows[0])
    widths = [max(len(header), *(len(row.get(header, "")) for row in rows)) for header in headers]
    print("    " + "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    print("    " + "  ".join("-" * w for w in widths))
    for row in rows:
        cells = (row.get(h, "").ljust(w) for h, w in zip(headers, widths, strict=True))
        print("    " + "  ".join(cells))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", required=True)
    parser.add_argument("--workgroup", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", help="a .sql file; every statement in it is run in order")
    source.add_argument("--sql", help="a single statement")
    parser.add_argument("--max-rows", type=int, default=25)
    args = parser.parse_args(argv)

    text = open(args.file).read() if args.file else args.sql
    statements = split_statements(text)
    if not statements:
        print("nothing to run", file=sys.stderr)
        return 1

    runner = AthenaRunner(database=args.database, workgroup=args.workgroup)
    failures = 0
    for index, sql in enumerate(statements, start=1):
        first_line = sql.splitlines()[0][:70]
        print(f"\n==> [{index}/{len(statements)}] {first_line}", flush=True)
        try:
            outcome = runner.execute(sql)
        except AthenaError as error:
            print(f"    FAILED: {error}", file=sys.stderr)
            failures += 1
            continue
        print(
            f"    {outcome.elapsed_ms} ms, {outcome.data_scanned_mb:.1f} MB scanned"
            f"  (execution {outcome.query_execution_id})"
        )
        _print_table(runner.fetch_rows(outcome.query_execution_id, max_rows=args.max_rows))

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
