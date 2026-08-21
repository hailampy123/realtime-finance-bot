"""Contract tests between the Python that describes the tables and the DDL that
creates them.

Neither can check the other at runtime -- one is a module, the other is text
sent to Athena -- so the agreement has to be asserted here or not at all.
"""

from __future__ import annotations

import re
from datetime import date

from awsnative import render
from awsnative.backfill.manifest import Outcome, plan
from awsnative.backfill.staging import KLINE_COLUMNS, TRADE_COLUMNS
from awsnative.backfill.tiers import Tier, files_for_window
from awsnative.bars import ADDITIVE_EXTREMA, ADDITIVE_SUM, NON_ADDITIVE

_COLUMN = re.compile(
    r"^\s+`?(\w+)`?\s+(string|timestamp|bigint|decimal|double|int|boolean)\b",
    re.MULTILINE,
)

DDL = dict(render.ddl_statements("fdai_native", "s3://bucket/"))
SILVER = DDL["010_silver_trades.sql"]
QUARANTINE = DDL["020_silver_trades_quarantine.sql"]
GOLD = DDL["030_gold_bars_1m.sql"]

TODAY = date(2026, 8, 17)


def columns(ddl: str) -> set[str]:
    return {match.group(1) for match in _COLUMN.finditer(ddl)}


def test_gold_declares_every_component_bars_py_names() -> None:
    """The drift tripwire. Add a measure to bars.py without adding the column
    and this fails; add the column without the measure and test_bars.py never
    exercises it."""
    declared = columns(GOLD)
    for name in (*ADDITIVE_SUM, *ADDITIVE_EXTREMA, *NON_ADDITIVE):
        assert name in declared, f"gold_bars_1m does not declare {name}"


def test_gold_stores_no_precomputed_ratio() -> None:
    """A vwap or imbalance column would be correct at exactly one grain and
    quietly wrong at every other (spec 5.3)."""
    declared = columns(GOLD)
    for forbidden in ("vwap", "flow_imbalance", "imbalance", "realized_vol"):
        assert forbidden not in declared, f"gold_bars_1m stores the ratio {forbidden}"


def test_gold_carries_the_fidelity_marker() -> None:
    """Without source_tier a two-year backtest silently mixes trade-derived and
    kline-derived bars, and the model appears to improve over time when all
    that improved is the input data (spec 6.4)."""
    assert "source_tier" in columns(GOLD)


def test_silver_partitioning_matches_the_dirty_partition_unit() -> None:
    """(instrument_id, day) is what makes "a backfill of one symbol-day rebuilds
    one symbol-day" true, and it is what the Gold merge joins on."""
    assert "PARTITIONED BY (instrument_id, day(event_ts))" in SILVER
    assert "PARTITIONED BY (instrument_id, day(window_end_ts))" in GOLD


# Every table a MERGE writes to. Iceberg is what MERGE needs; a table that is only
# ever read does not need it and pays for it in metadata (D1/D2). native_health_metrics
# is written by INSERT (not MERGE) but is still an Iceberg table.
MERGE_TARGET_DDL = (
    "010_silver_trades.sql",
    "020_silver_trades_quarantine.sql",
    "030_gold_bars_1m.sql",
    "040_backfill_manifest.sql",
    "052_silver_perp_context.sql",
    "053_silver_macro.sql",
    "054_native_health_metrics.sql",
)

# Read-only sources. Staging is emptied and rewritten per run by the loader,
# backfill_outcomes is written by Step Functions' ResultWriter, and the two Bronze
# tables are written by the enrichment Lambdas -- none is a MERGE target, and all
# are external text tables on purpose (D1: append-only needs no Iceberg).
READ_ONLY_DDL = (
    "041_archive_staging_klines.sql",
    "042_archive_staging_trades.sql",
    "043_backfill_outcomes.sql",
    "050_bronze_perp_context.sql",
    "051_bronze_macro_observations.sql",
)


def test_every_ddl_file_is_classified() -> None:
    """A new table must be a declared MERGE target or a declared read-only source.

    Without this, adding a table silently opts it out of both checks below.
    """
    assert set(MERGE_TARGET_DDL) | set(READ_ONLY_DDL) == set(DDL)


def test_every_merge_target_is_iceberg() -> None:
    """Bronze is deliberately plain Parquet (D1); everything a MERGE writes to is
    Iceberg, because MERGE, atomic commits and snapshot isolation are what
    Iceberg is here for."""
    for name in MERGE_TARGET_DDL:
        assert "'table_type'        = 'ICEBERG'" in DDL[name], f"{name} is not Iceberg"


def test_no_read_only_staging_table_is_iceberg() -> None:
    """Iceberg on a table the loader overwrites wholesale would accumulate a
    snapshot per run for time travel nobody reads, and cannot be written by a
    Lambda that has no Iceberg library."""
    for name in READ_ONLY_DDL:
        assert "ICEBERG" not in DDL[name], f"{name} should be an external table"
        assert "CREATE EXTERNAL TABLE" in DDL[name]


def test_quarantine_keeps_price_and_size_as_strings() -> None:
    """A row lands in quarantine precisely because a value would not cast, so a
    DECIMAL column here could not hold the thing you came to look at."""
    assert re.search(r"^\s+price\s+string", QUARANTINE, re.MULTILINE)
    assert re.search(r"^\s+`size`\s+string", QUARANTINE, re.MULTILINE)
    # Silver, having only cast-able rows, does not have that problem.
    assert re.search(r"^\s+price\s+decimal", SILVER, re.MULTILINE)


def test_quarantine_is_keyed_on_a_null_safe_hash() -> None:
    """A row can be quarantined FOR having a NULL venue, and NULL = NULL is
    never true -- so keying on (venue, trade_id) would re-insert those rows on
    every five-minute run, forever."""
    assert "row_key" in columns(QUARANTINE)
    quarantine_merge = render.merge_statements("fdai_native")[render.MERGE_QUARANTINE]
    assert "ON t.row_key = s.row_key" in quarantine_merge
    assert "md5(to_utf8(" in quarantine_merge


def test_silver_keeps_both_the_exact_and_the_convenient_timestamp() -> None:
    """from_unixtime truncates to milliseconds, so event_ts alone would quietly
    lose the microseconds the wire format carries."""
    declared = columns(SILVER)
    assert {"event_ts", "event_ts_us", "ingest_ts", "ingest_ts_us"} <= declared


def test_silver_columns_cover_the_bronze_contract() -> None:
    """Every field the encoder writes, minus schema_version, which describes the
    envelope rather than the trade and stops being useful once parsed."""
    declared = columns(SILVER)
    expected = {
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        "ingest_ts_us",
        "price",
        "size",
        "side",
        "sequence",
        "is_backfill",
        "source",
    }
    assert expected <= declared


# --- stage N4: the staging tables ------------------------------------------
#
# These three assertions are the only thing standing between a reordered tuple and
# silently shifted data. The staging tables have no header row, so Athena assigns
# columns by POSITION; if staging.py and the DDL disagreed, every value would land
# in the wrong column and most of the types would still fit, because every staging
# column is declared `string`.


def ordered_columns(ddl: str) -> list[str]:
    """Declared column names, in declaration order."""
    return [match.group(1) for match in _COLUMN.finditer(ddl)]


def test_kline_staging_column_order_matches_the_writer() -> None:
    assert ordered_columns(DDL["041_archive_staging_klines.sql"]) == list(KLINE_COLUMNS)


def test_trade_staging_column_order_matches_the_writer() -> None:
    assert ordered_columns(DDL["042_archive_staging_trades.sql"]) == list(TRADE_COLUMNS)


def test_every_staging_column_is_declared_string() -> None:
    """The archive's decimals must reach DECIMAL(38, 18) in the merge without
    passing through a float on the way. Declaring them string is what guarantees
    Athena parses them once, at the cast, rather than twice."""
    for name in ("041_archive_staging_klines.sql", "042_archive_staging_trades.sql"):
        for column, kind in _COLUMN.findall(DDL[name]):
            assert kind == "string", f"{name}.{column} is {kind}, expected string"


def test_outcomes_table_declares_exactly_what_the_lambda_returns() -> None:
    """A JsonSerDe matches by NAME, so a mismatch here is a silently NULL column
    rather than shifted data -- which is harder to notice, not easier."""
    item = plan(
        files_for_window(
            [("BTC-USD", "BTCUSDT")], Tier.DEEP, date(2025, 6, 1), date(2025, 6, 30), today=TODAY
        ),
        already_done=frozenset(),
    )[0]
    returned = {
        **item.to_json(),
        **Outcome.done(archive_key="k", sha256="a", row_count=1).to_json(),
    }
    assert set(ordered_columns(DDL["043_backfill_outcomes.sql"])) == set(returned)


# Every table under maintenance gets a bounded retention window in its own
# DDL rather than an ALTER TABLE step after the fact (design 2026-08-17
# section 8.3). 5 days outlives nothing here -- the sandbox is wiped weekly
# -- so 1 hour is the deliberate choice: enough to debug a bad merge,
# short enough that metadata never accumulates across a session.
MAINTAINED_DDL = (
    "010_silver_trades.sql",
    "020_silver_trades_quarantine.sql",
    "030_gold_bars_1m.sql",
    "052_silver_perp_context.sql",
    "053_silver_macro.sql",
    "054_native_health_metrics.sql",
)


def test_maintained_tables_set_vacuum_retention_properties() -> None:
    for name in MAINTAINED_DDL:
        assert "'vacuum_max_snapshot_age_seconds' = '3600'" in DDL[name], name
        assert "'vacuum_min_snapshots_to_keep'    = '5'" in DDL[name], name


def test_native_health_metrics_is_iceberg_and_declares_every_column() -> None:
    ddl = DDL["054_native_health_metrics.sql"]
    assert "'table_type'        = 'ICEBERG'" in ddl
    for column in (
        "metric_ts",
        "table_name",
        "tier",
        "row_count",
        "file_count",
        "avg_file_size_mb",
        "small_file_pct",
        "delete_file_count",
        "snapshot_count",
        "oldest_snapshot_age_seconds",
        "freshness_lag_seconds",
        "quarantine_rate_pct",
    ):
        assert column in columns(ddl), f"native_health_metrics missing {column}"
