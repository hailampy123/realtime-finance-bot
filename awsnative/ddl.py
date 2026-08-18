"""Create the Iceberg tables. Idempotent; safe to re-run.

WHY THIS IS NOT TERRAFORM. silver_trades and gold_bars_1m are partitioned by
day(event_ts) -- an Iceberg partition transform. Glue's CreateTable API accepts
identity partitions only and has no way to express a transform at all, so
aws_glue_catalog_table cannot create these tables however it is configured.
The DDL has to go through Athena, and something has to send it.

That costs the letter of spec section 8: `terraform apply` alone no longer
produces the whole stage. `make up-aws` still does, and it already stood
outside Terraform for docker build and push for the same reason -- some things
are not resources.

The alternative was a null_resource with a local-exec provisioner, which keeps
the claim literally true and hides a failing CREATE TABLE behind an exit code
in the middle of an apply. Being able to re-run one command and read the actual
Athena error is worth more than the claim.
"""

from __future__ import annotations

import argparse
import sys

from awsnative.athena import AthenaError, AthenaRunner
from awsnative.render import ddl_statements


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", required=True, help="Glue database, e.g. fdai_native")
    parser.add_argument("--workgroup", required=True, help="Athena workgroup, e.g. fdai-native")
    parser.add_argument("--bucket", required=True, help="lake bucket name, no s3:// prefix")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the rendered DDL and exit without touching AWS",
    )
    args = parser.parse_args(argv)

    statements = ddl_statements(args.database, f"s3://{args.bucket}/")

    if args.dry_run:
        for name, sql in statements:
            print(f"-- {name}\n{sql}\n")
        return 0

    runner = AthenaRunner(database=args.database, workgroup=args.workgroup)
    for name, sql in statements:
        print(f"==> {name}", flush=True)
        try:
            outcome = runner.execute(sql)
        except AthenaError as error:
            # CREATE TABLE IF NOT EXISTS is meant to swallow this, but Athena
            # has historically raised AlreadyExistsException for Iceberg tables
            # anyway. Treat it as the success it is; anything else is real.
            if "AlreadyExists" in str(error):
                print("    already exists, unchanged")
                continue
            print(f"    FAILED: {error}", file=sys.stderr)
            return 1
        print(f"    ok ({outcome.elapsed_ms} ms)")

    print("\nTables ready. Verification queries: awsnative/sql/verify_silver_gold.sql")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
