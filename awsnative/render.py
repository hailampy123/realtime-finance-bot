"""Turn the .sql files under awsnative/sql into executable statements.

The transforms live as parameterised .sql files rather than as strings in
Python or HCL because two things need them and neither may hold the original:
Terraform embeds the merges in the Step Functions definition, and this module
runs the DDL and the verification queries. One file, two consumers, no copy.

Two kinds of placeholder, and the distinction matters:

    ${database}, ${warehouse}, ${lookback_days}   deploy-time. Rendered here
        and by Terraform's templatefile(), which uses the same ${...} syntax --
        that compatibility is the reason this module uses string.Template
        rather than f-strings or Jinja.

    ${valid_expr}, ${dirty_cte}                   composition. Whole SQL
        fragments spliced in, so that a predicate or a partition selector has
        exactly one definition shared by every file that needs it.

Run-time values would be a third kind and there are none yet: nothing in the
micro-batch varies per execution. Stage N5's prepared statements introduce
real ? parameters, bound by Athena rather than by string substitution, which
is where that distinction starts to carry weight.
"""

from __future__ import annotations

from pathlib import Path
from string import Template

SQL_DIR = Path(__file__).resolve().parent / "sql"
DDL_DIR = SQL_DIR / "ddl"
FRAGMENT_DIR = SQL_DIR / "fragments"

# Every placeholder any .sql file may use. A file introducing a name outside
# this set, or a caller forgetting one a file needs, fails in
# tests/awsnative/test_render.py rather than as a half-substituted query.
KNOWN_PLACEHOLDERS = frozenset(
    {
        "database",
        "warehouse",
        "lookback_days",
        "valid_expr",
        "dirty_cte",
        "projection_start_date",
        "instrument_id",
    }
)

# Applied in name order: Silver before quarantine before Gold, then stage N4's
# manifest and staging tables. The numeric prefixes are the ordering, so a future
# table slots in without renaming.
DDL_FILES = (
    "010_silver_trades.sql",
    "020_silver_trades_quarantine.sql",
    "030_gold_bars_1m.sql",
    "040_backfill_manifest.sql",
    "041_archive_staging_klines.sql",
    "042_archive_staging_trades.sql",
    "043_backfill_outcomes.sql",
    "050_bronze_perp_context.sql",
    "051_bronze_macro_observations.sql",
    "052_silver_perp_context.sql",
    "053_silver_macro.sql",
)

# The three statements the micro-batch state machine runs. Terraform reads the
# same files; keep the names in sync with infra/modules/native_medallion.
MERGE_SILVER = "merge_silver_trades.sql"
MERGE_QUARANTINE = "merge_silver_quarantine.sql"
MERGE_GOLD = "merge_gold_bars_1m.sql"

# Stage N4's three statements. The backfill state machine runs them in this order:
# outcomes first so the manifest records the run even if a merge then fails, then
# the two tier merges. Terraform reads the same files.
MERGE_MANIFEST_OUTCOMES = "merge_manifest_outcomes.sql"
MERGE_SILVER_ARCHIVE = "merge_silver_from_archive.sql"
MERGE_GOLD_ARCHIVE = "merge_gold_from_archive.sql"

# Slices E1 and E3. Each runs in its own state machine
# (infra/modules/native_enrichment), on the same schedule as the collector that
# feeds it, rather than as a state inside the trade micro-batch: folding macro
# into a 5-minute cadence would rescan bronze_macro_observations and
# silver_macro in full for data that changes at most once a day, and a shared
# machine would make microbatch_enabled and enrichment_enabled stop being
# independent switches.
MERGE_PERP_CONTEXT = "merge_silver_perp_context.sql"
MERGE_MACRO = "merge_silver_macro.sql"


def read_sql(relative: str) -> str:
    """Read a .sql file relative to awsnative/sql, unrendered."""
    return (SQL_DIR / relative).read_text()


def placeholders_in(text: str) -> set[str]:
    """The ${...} names a template uses."""
    return set(Template(text).get_identifiers())


def render(text: str, **params: object) -> str:
    """Substitute every placeholder, raising if one is missing.

    substitute(), never safe_substitute(): a silently unrendered ${database}
    would reach Athena as a syntax error at 3am instead of here.
    """
    return Template(text).substitute(**params)


def valid_expr() -> str:
    """The shared validity predicate, as a bare boolean expression.

    Callers wrap it themselves -- COALESCE(x, false) for Silver and
    NOT COALESCE(x, false) for quarantine. The wrapping lives in the two merge
    files rather than here so that both languages that render these templates
    apply it identically by not being involved.

    Deliberately NOT stripped. Terraform's file() does not strip, and these two
    renderers must agree byte for byte or the "one definition" claim is only
    approximately true. scripts/native_render_parity.sh is what enforces it.
    """
    return (FRAGMENT_DIR / "valid_trade.sql").read_text()


def dirty_from_bronze(database: str, lookback_days: int) -> str:
    """The micro-batch's dirty-partition CTE body. Unstripped; see valid_expr."""
    return render(
        (FRAGMENT_DIR / "dirty_from_bronze.sql").read_text(),
        database=database,
        lookback_days=lookback_days,
    )


# The earliest partition Athena's projection will compute a location for. Before
# it, a query returns nothing rather than an error, so it only has to be early
# enough -- and every account this runs in is younger than this date.
DEFAULT_PROJECTION_START_DATE = "2026-01-01"


def ddl_statements(
    database: str,
    warehouse: str,
    projection_start_date: str = DEFAULT_PROJECTION_START_DATE,
) -> list[tuple[str, str]]:
    """(filename, sql) for every CREATE TABLE, in application order.

    `warehouse` is an s3:// prefix WITH a trailing slash; the DDL appends the
    table name to it.

    `projection_start_date` is used only by the two Bronze tables this module
    creates. Every parameter is passed to every file: string.Template raises on a
    placeholder a caller forgot but ignores one a file does not use, so one call
    site serves eleven files without a per-file parameter table.
    """
    if not warehouse.endswith("/"):
        warehouse += "/"
    return [
        (
            name,
            render(
                (DDL_DIR / name).read_text(),
                database=database,
                warehouse=warehouse,
                projection_start_date=projection_start_date,
            ),
        )
        for name in DDL_FILES
    ]


def merge_statements(database: str, lookback_days: int = 1) -> dict[str, str]:
    """The three micro-batch merges, keyed by filename.

    Terraform renders these same files with the same parameters for the state
    machine definition. This function exists for tests and for running a merge
    by hand against a dev workgroup -- it is not what production executes.
    """
    expr = valid_expr()
    return {
        MERGE_SILVER: render(
            read_sql(MERGE_SILVER),
            database=database,
            lookback_days=lookback_days,
            valid_expr=expr,
        ),
        MERGE_QUARANTINE: render(
            read_sql(MERGE_QUARANTINE),
            database=database,
            lookback_days=lookback_days,
            valid_expr=expr,
        ),
        MERGE_GOLD: render(
            read_sql(MERGE_GOLD),
            database=database,
            dirty_cte=dirty_from_bronze(database, lookback_days),
        ),
    }


def backfill_statements(database: str) -> dict[str, str]:
    """Stage N4's three statements, keyed by filename.

    No `lookback_days` and no dirty CTE: a backfill's scope is whatever the loader
    staged, which the state machine emptied beforehand. The window lives in the
    seed step (awsnative/backfill/seed.py), not in the SQL, so re-running a merge
    cannot widen or narrow what a run covered.
    """
    return {
        name: render(read_sql(name), database=database)
        for name in (MERGE_MANIFEST_OUTCOMES, MERGE_SILVER_ARCHIVE, MERGE_GOLD_ARCHIVE)
    }


def enrichment_statements(database: str, lookback_days: int = 1) -> dict[str, str]:
    """Slices E1 and E3, keyed by filename.

    The macro merge takes no lookback: it reads whatever vintages Bronze holds and
    inserts only values that are new for an observation, so widening the window
    would change nothing except what it scans.
    """
    return {
        MERGE_PERP_CONTEXT: render(
            read_sql(MERGE_PERP_CONTEXT), database=database, lookback_days=lookback_days
        ),
        MERGE_MACRO: render(read_sql(MERGE_MACRO), database=database),
    }
