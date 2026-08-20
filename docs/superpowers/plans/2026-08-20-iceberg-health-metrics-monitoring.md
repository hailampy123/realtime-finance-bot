# Iceberg Health Metrics Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish per-table health metrics (freshness, quarantine rate, file/delete-file counts, snapshot age) to CloudWatch with alarms on the ones that matter, and keep a queryable history of them in a new Iceberg table.

**Architecture:** A new Lambda (`health_metrics_handler`) runs one Athena SELECT per invocation (UNION ALL across the tables its caller owns), publishes the results to CloudWatch, and appends them to a new table `native_health_metrics`. It is invoked as a tail state — after maintenance, which is itself after the merge — in each of the three state machines Plan 1 (`2026-08-19-iceberg-maintenance-extension.md`) already modified. One new Terraform module, `native_monitoring`, owns the Lambda, its role, the SNS topic, and every CloudWatch alarm.

**Tech Stack:** Terraform (AWS provider), Python 3.12 (Lambda, `boto3`, `awsnative.athena.AthenaRunner`, `awsnative.render`), Trino/Athena SQL.

**Spec:** [`docs/superpowers/specs/2026-08-19-iceberg-housekeeping-monitoring-design.md`](../specs/2026-08-19-iceberg-housekeeping-monitoring-design.md), section 4 only (not section 5, the QuickSight dashboard — a separate plan once `native_health_metrics` has real data to point it at).

**Depends on:** [`2026-08-19-iceberg-maintenance-extension.md`](2026-08-19-iceberg-maintenance-extension.md) (Plan 1). Every Terraform task below assumes Plan 1 has already been applied to `infra/modules/native_medallion/main.tf` and `infra/modules/native_enrichment/main.tf` — the exact `MaintenanceDone`/`VacuumSilverMacro` states this plan rewires only exist after Plan 1 lands. If Plan 1 has not been applied yet, apply it first.

## Global Constraints

- `native_health_metrics` is written by **three different state machines**, unlike every other Iceberg table in this repo, which has exactly one writer. This plan uses a plain `INSERT`, not a `MERGE`, specifically because of that: a duplicate reading from a retried commit is a cosmetic blip in an observability table, not corrupted business data, so the existing `Retry`-with-backoff pattern (already used for every Athena Task in this repo) is a sufficient mitigation without new commit-conflict handling. See Task 3's docstring for the full reasoning.
- Deviation from this repo's own stated principle in `awsnative/athena.py` ("Step Functions calls Athena directly... that is the whole reason stage N2 needs no Lambda"): `CollectHealthMetrics` IS a Lambda, chosen deliberately over pure ASL after a specific trade-off discussion (row-to-metric mapping gets real pytest coverage instead of living only in Step Functions JSON at fixed array positions). It runs strictly after the merge and maintenance already committed, not in the write-critical path that principle was stated for.
- A genuine circular-dependency risk and its resolution: `native_monitoring`'s `ExecutionsFailed` alarms need the other two modules' state-machine ARNs, and `native_medallion`/`native_enrichment` need `native_monitoring`'s Lambda ARN. Breaking this the same way `native_medallion`'s own `local.state_machine_arn` already does — construct the ARN from `project`/`region`/`account_id` rather than take it as a module output — keeps the dependency graph one-directional. Do not "simplify" this to a direct module reference; it will not `terraform plan`.
- M2 (spec 2026-08-17, still open): whether Athena's `"table$files"` exposes a `content` column distinguishing delete files from data files. This plan's SQL assumes it does; Task 9 verifies this live and names the fallback.
- Every `.sql` file under `awsnative/sql/` must use only placeholders in `render.KNOWN_PLACEHOLDERS` (`tests/awsnative/test_render.py::test_only_known_placeholders` enforces this automatically).

---

### Task 1: `native_health_metrics` DDL and its place in `render.py`

**Files:**
- Create: `awsnative/sql/ddl/054_native_health_metrics.sql`
- Modify: `awsnative/render.py` — `DDL_FILES` tuple, `MAINTAINED_TABLES` tuple, `MAINTENANCE_PARTITION_PREDICATES` dict (all three added by Plan 1)
- Test: `tests/awsnative/test_render.py`, `tests/awsnative/test_sql_contracts.py`

**Interfaces:**
- Produces: table `native_health_metrics` (12 columns: `metric_ts`, `table_name`, `tier`, `row_count`, `file_count`, `avg_file_size_mb`, `small_file_pct`, `delete_file_count`, `snapshot_count`, `oldest_snapshot_age_seconds`, `freshness_lag_seconds`, `quarantine_rate_pct`), partitioned by `day(metric_ts)` — no separate stored date column, matching how `silver_trades` partitions on `day(event_ts)` without a redundant `event_date` column (a refinement over the spec's schema sketch, which listed a stored `metric_date`; noted in this plan's self-review).

- [ ] **Step 1: Write the failing tests**

Append to `tests/awsnative/test_render.py`:

```python
def test_native_health_metrics_is_in_ddl_files_and_maintained_tables() -> None:
    assert "054_native_health_metrics.sql" in render.DDL_FILES
    assert "native_health_metrics" in render.MAINTAINED_TABLES


def test_native_health_metrics_gets_optimize_and_vacuum() -> None:
    """Written by three state machines at up to 5-minute cadence -- it reaches
    the small-file threshold faster than any single-writer table, so unlike
    silver_macro it needs OPTIMIZE too."""
    statements = render.maintenance_statements("fdai_native")
    assert statements["native_health_metrics"]["optimize"] is not None
    assert "metric_ts" in statements["native_health_metrics"]["optimize"]
```

Append to `tests/awsnative/test_sql_contracts.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/awsnative/test_render.py tests/awsnative/test_sql_contracts.py -k native_health_metrics -v`
Expected: FAIL — `KeyError: '054_native_health_metrics.sql'` and `AssertionError` on the `MAINTAINED_TABLES` check.

- [ ] **Step 3: Create the DDL file**

`awsnative/sql/ddl/054_native_health_metrics.sql`:

```sql
-- Health metrics: one row per (table, tick), written by the CollectHealthMetrics
-- tail state in three different state machines (spec
-- 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4). The only
-- Iceberg table in this repo with more than one writer -- see the plain
-- INSERT (not MERGE) in awsnative/monitoring/collect.py for why that is safe
-- here specifically.
--
-- Partitioned on day(metric_ts) alone, like silver_trades on day(event_ts):
-- the partition value is derived from the timestamp column, so there is no
-- separate stored date column to keep in sync with it.
CREATE TABLE IF NOT EXISTS ${database}.native_health_metrics (
    metric_ts                   timestamp,
    table_name                  string,
    tier                        string,
    row_count                   bigint,
    file_count                  bigint,
    avg_file_size_mb            double,
    small_file_pct              double,
    delete_file_count           bigint,
    snapshot_count              bigint,
    oldest_snapshot_age_seconds bigint,
    freshness_lag_seconds       bigint,
    quarantine_rate_pct         double
)
PARTITIONED BY (day(metric_ts))
LOCATION '${warehouse}native_health_metrics/'
TBLPROPERTIES (
    'table_type'                      = 'ICEBERG',
    'format'                          = 'parquet',
    'write_compression'               = 'snappy',
    'vacuum_max_snapshot_age_seconds' = '3600',
    'vacuum_min_snapshots_to_keep'    = '5'
)
```

- [ ] **Step 4: Add it to `render.py`'s `DDL_FILES`**

In `awsnative/render.py`, change:

```python
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
```

to:

```python
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
    "054_native_health_metrics.sql",
)
```

- [ ] **Step 5: Add it to `MAINTAINED_TABLES` and give it a partition predicate**

Plan 1 added `MAINTENANCE_PARTITION_PREDICATES` and `MAINTAINED_TABLES` to the end of `render.py`. Change:

```python
MAINTENANCE_PARTITION_PREDICATES: dict[str, str] = {
    "silver_trades": "event_ts >= current_date - interval '${lookback_days}' day",
    "silver_trades_quarantine": (
        "ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')"
    ),
    "gold_bars_1m": "window_end_ts >= current_date - interval '${lookback_days}' day",
    "silver_perp_context": "snapshot_ts >= current_date - interval '${lookback_days}' day",
}

MAINTAINED_TABLES = (
    "silver_trades",
    "silver_trades_quarantine",
    "gold_bars_1m",
    "silver_perp_context",
    "silver_macro",
)
```

to:

```python
MAINTENANCE_PARTITION_PREDICATES: dict[str, str] = {
    "silver_trades": "event_ts >= current_date - interval '${lookback_days}' day",
    "silver_trades_quarantine": (
        "ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')"
    ),
    "gold_bars_1m": "window_end_ts >= current_date - interval '${lookback_days}' day",
    "silver_perp_context": "snapshot_ts >= current_date - interval '${lookback_days}' day",
    # Written by three state machines at up to 5-minute cadence -- faster
    # than any single-writer table -- so unlike silver_macro this needs
    # OPTIMIZE, not just VACUUM.
    "native_health_metrics": "metric_ts >= current_date - interval '${lookback_days}' day",
}

MAINTAINED_TABLES = (
    "silver_trades",
    "silver_trades_quarantine",
    "gold_bars_1m",
    "silver_perp_context",
    "silver_macro",
    "native_health_metrics",
)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/awsnative/test_render.py tests/awsnative/test_sql_contracts.py -v`
Expected: PASS, all tests in both files.

- [ ] **Step 7: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/sql/ddl/054_native_health_metrics.sql awsnative/render.py tests/awsnative/test_render.py tests/awsnative/test_sql_contracts.py
git commit -m "feat(awsnative): add native_health_metrics table"
```

---

### Task 2: The health-metrics SELECT template and its renderer

**Files:**
- Create: `awsnative/sql/health_metrics_row.sql`
- Modify: `awsnative/render.py` — add `Sequence` import, `KNOWN_PLACEHOLDERS` entries, `health_metrics_select_statement()`
- Test: `tests/awsnative/test_render.py`

**Interfaces:**
- Produces: `render.health_metrics_select_statement(database: str, tables: Sequence[str], lookback_days: int = 1) -> str`. Consumed by Task 3's Lambda, not by Terraform — this is the first SQL in this repo that a Lambda renders and runs itself rather than Step Functions.

- [ ] **Step 1: Write the failing tests**

Append to `tests/awsnative/test_render.py`:

```python
def test_health_metrics_select_covers_every_requested_table() -> None:
    from awsnative.athena import split_statements

    sql = render.health_metrics_select_statement("fdai_native", ["silver_trades", "gold_bars_1m"])
    assert len(split_statements(sql)) == 1
    assert "'silver_trades'" in sql
    assert "'gold_bars_1m'" in sql
    assert "UNION ALL" in sql


def test_health_metrics_select_has_no_freshness_for_quarantine() -> None:
    sql = render.health_metrics_select_statement("fdai_native", ["silver_trades_quarantine"])
    assert "CAST(NULL AS bigint) AS freshness_lag_seconds" in sql


def test_health_metrics_select_has_quarantine_rate_only_for_quarantine() -> None:
    sql = render.health_metrics_select_statement("fdai_native", ["silver_trades", "silver_trades_quarantine"])
    assert sql.count("CAST(NULL AS double) AS quarantine_rate_pct") == 1
    assert "silver_trades_quarantine" in sql and "NULLIF(" in sql


def test_health_metrics_select_uses_macros_vintage_date_for_freshness() -> None:
    """Macro has no event/ingest timestamp; freshness reads off vintage_date,
    the same column 02_freshness.sql already uses for this table."""
    sql = render.health_metrics_select_statement("fdai_native", ["silver_macro"])
    assert "CAST(max(vintage_date) AS TIMESTAMP)" in sql


def test_health_metrics_select_renders_with_no_placeholder_left() -> None:
    sql = render.health_metrics_select_statement("fdai_native", list(render.HEALTH_METRICS_TIER), lookback_days=2)
    assert "${" not in sql
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/awsnative/test_render.py -k health_metrics_select -v`
Expected: FAIL — `AttributeError: module 'awsnative.render' has no attribute 'health_metrics_select_statement'`.

- [ ] **Step 3: Create the row template**

`awsnative/sql/health_metrics_row.sql`:

```sql
-- One row of health metrics for one table, composed with others via
-- UNION ALL by render.py's health_metrics_select_statement() (spec
-- 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4.1).
--
-- freshness_expr and quarantine_expr are whole, already-resolved SQL
-- expressions, not bare column names: most tables have no meaningful
-- reading for one or the other (silver_trades_quarantine has no freshness
-- concept of its own; every table except silver_trades_quarantine has no
-- quarantine rate), so the caller renders CAST(NULL AS ...) for those and
-- splices the result in -- the same two-step composition
-- fragments/dirty_from_bronze.sql uses for dirty_cte.
--
-- delete_file_count assumes "$files" exposes a `content` column
-- distinguishing delete files from data files (spec 2026-08-17 assumption
-- M2, unverified as of this writing -- see this plan's deploy task).
SELECT
    current_timestamp                                                     AS metric_ts,
    '${table}'                                                            AS table_name,
    '${tier}'                                                             AS tier,
    (SELECT count(*) FROM ${database}.${table})                          AS row_count,
    (SELECT count(*) FROM ${database}."${table}$files")                  AS file_count,
    (SELECT avg(file_size_in_bytes) / 1e6 FROM ${database}."${table}$files")
                                                                           AS avg_file_size_mb,
    (SELECT 100.0 * sum(CASE WHEN file_size_in_bytes < 100000000 THEN 1 ELSE 0 END) / count(*)
       FROM ${database}."${table}$files")                                AS small_file_pct,
    (SELECT count(*) FROM ${database}."${table}$files" WHERE content <> 0)
                                                                           AS delete_file_count,
    (SELECT count(*) FROM ${database}."${table}$snapshots")              AS snapshot_count,
    (SELECT to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
       FROM ${database}."${table}$snapshots")                            AS oldest_snapshot_age_seconds,
    ${freshness_expr}                                                    AS freshness_lag_seconds,
    ${quarantine_expr}                                                   AS quarantine_rate_pct
```

- [ ] **Step 4: Add the new placeholders to `KNOWN_PLACEHOLDERS`**

Plan 1 already added `table` and `partition_predicate`. In `awsnative/render.py`, change:

```python
KNOWN_PLACEHOLDERS = frozenset(
    {
        "database",
        "warehouse",
        "lookback_days",
        "valid_expr",
        "dirty_cte",
        "projection_start_date",
        "instrument_id",
        "table",
        "partition_predicate",
    }
)
```

to:

```python
KNOWN_PLACEHOLDERS = frozenset(
    {
        "database",
        "warehouse",
        "lookback_days",
        "valid_expr",
        "dirty_cte",
        "projection_start_date",
        "instrument_id",
        "table",
        "partition_predicate",
        "tier",
        "freshness_expr",
        "quarantine_expr",
    }
)
```

- [ ] **Step 5: Add the `Sequence` import**

In `awsnative/render.py`, change:

```python
from __future__ import annotations

from pathlib import Path
from string import Template
```

to:

```python
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from string import Template
```

- [ ] **Step 6: Append the health-metrics renderer**

Append to the end of `awsnative/render.py` (after Task 1's `MAINTAINED_TABLES` and Plan 1's `maintenance_statements()`):

```python
# --- health metrics collection, spec 2026-08-19 section 4 ------------------
HEALTH_METRICS_ROW = "health_metrics_row.sql"

HEALTH_METRICS_FRESHNESS_EXPR: dict[str, str] = {
    "silver_trades": (
        "(SELECT to_unixtime(current_timestamp) - to_unixtime(max(event_ts)) "
        "FROM ${database}.silver_trades)"
    ),
    "gold_bars_1m": (
        "(SELECT to_unixtime(current_timestamp) - to_unixtime(max(window_end_ts)) "
        "FROM ${database}.gold_bars_1m)"
    ),
    "silver_perp_context": (
        "(SELECT to_unixtime(current_timestamp) - to_unixtime(max(snapshot_ts)) "
        "FROM ${database}.silver_perp_context)"
    ),
    "silver_macro": (
        "(SELECT to_unixtime(current_timestamp) - to_unixtime(CAST(max(vintage_date) AS TIMESTAMP)) "
        "FROM ${database}.silver_macro)"
    ),
}
# silver_trades_quarantine and native_health_metrics have no entry: neither
# has a meaningful "how stale is this" reading of its own (one records how
# something else fell behind; the other IS the record of that). Their rows
# render CAST(NULL AS bigint) instead.

HEALTH_METRICS_QUARANTINE_EXPR = (
    "(SELECT 100.0 * "
    "(SELECT count(*) FROM ${database}.silver_trades_quarantine "
    "WHERE ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')) "
    "/ NULLIF((SELECT count(*) FROM ${database}.silver_trades "
    "WHERE event_ts >= current_date - interval '${lookback_days}' day), 0))"
)
# Only silver_trades_quarantine's row uses this. Every other table renders
# CAST(NULL AS double) instead.

HEALTH_METRICS_TIER: dict[str, str] = {
    "silver_trades": "fast",
    "silver_trades_quarantine": "fast",
    "gold_bars_1m": "fast",
    "silver_perp_context": "fast",
    "silver_macro": "slow",
    "native_health_metrics": "fast",
}


def health_metrics_select_statement(
    database: str, tables: Sequence[str], lookback_days: int = 1
) -> str:
    """One SELECT, UNION ALL across `tables`, one row of KPIs per table.

    `tables` lets each state machine ask only for the tables it writes:
    the microbatch asks for its three fast tables, merge-perp for
    silver_perp_context, merge-macro for silver_macro. The health-metrics
    Lambda calls this directly; Terraform never renders it, because the
    Lambda -- not a Step Functions Athena task -- is what runs this query
    (see the "why a Lambda here" note in awsnative/monitoring/collect.py).
    """
    row_template = read_sql(HEALTH_METRICS_ROW)
    rows = []
    for table in tables:
        freshness_template = HEALTH_METRICS_FRESHNESS_EXPR.get(table)
        freshness_expr = (
            render(freshness_template, database=database)
            if freshness_template is not None
            else "CAST(NULL AS bigint)"
        )
        quarantine_expr = (
            render(HEALTH_METRICS_QUARANTINE_EXPR, database=database, lookback_days=lookback_days)
            if table == "silver_trades_quarantine"
            else "CAST(NULL AS double)"
        )
        rows.append(
            render(
                row_template,
                database=database,
                table=table,
                tier=HEALTH_METRICS_TIER[table],
                freshness_expr=freshness_expr,
                quarantine_expr=quarantine_expr,
            )
        )
    return "\nUNION ALL\n".join(rows)
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/awsnative/test_render.py -v`
Expected: PASS, all tests in the file.

- [ ] **Step 8: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/sql/health_metrics_row.sql awsnative/render.py tests/awsnative/test_render.py
git commit -m "feat(awsnative): add health-metrics SELECT template and renderer"
```

---

### Task 3: The health-metrics Lambda

**Files:**
- Create: `awsnative/monitoring/__init__.py`
- Create: `awsnative/monitoring/collect.py`
- Test: `tests/awsnative/monitoring/__init__.py`
- Test: `tests/awsnative/monitoring/test_collect.py`

**Interfaces:**
- Consumes: `awsnative.render.health_metrics_select_statement` (Task 2), `awsnative.athena.AthenaRunner`/`AthenaRunner.fetch_rows` (existing).
- Produces: `metric_data(rows: list[dict[str, str]]) -> list[dict[str, Any]]`, `insert_statement(database: str, rows: list[dict[str, str]]) -> str`, `health_metrics_handler(event: dict[str, Any], context: object = None) -> dict[str, str]` — the Lambda entry point, `Event: {"database": str, "workgroup": str, "tables": list[str]}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/awsnative/monitoring/__init__.py` (empty file).

Create `tests/awsnative/monitoring/test_collect.py`:

```python
from __future__ import annotations

from awsnative.athena import split_statements
from awsnative.monitoring.collect import _METRIC_COLUMNS, insert_statement, metric_data


def test_metric_data_skips_null_cells() -> None:
    rows = [{"table_name": "silver_trades", "row_count": "100", "quarantine_rate_pct": ""}]
    entries = metric_data(rows)
    names = {e["MetricName"] for e in entries}
    assert "RowCount" in names
    assert "QuarantineRatePct" not in names


def test_metric_data_dimensions_by_table_name() -> None:
    rows = [{"table_name": "gold_bars_1m", "row_count": "5"}]
    entries = metric_data(rows)
    assert entries[0]["Dimensions"] == [{"Name": "TableName", "Value": "gold_bars_1m"}]


def test_metric_data_uses_seconds_unit_for_seconds_columns() -> None:
    rows = [{"table_name": "silver_trades", "freshness_lag_seconds": "42"}]
    entries = metric_data(rows)
    assert entries[0]["Unit"] == "Seconds"


def test_metric_data_pascal_cases_metric_names() -> None:
    rows = [{"table_name": "silver_trades", "avg_file_size_mb": "1.5"}]
    entries = metric_data(rows)
    assert entries[0]["MetricName"] == "AvgFileSizeMb"


_FULL_ROW = {
    "metric_ts": "2026-08-19 12:00:00.000",
    "table_name": "silver_trades",
    "tier": "fast",
    **dict.fromkeys(_METRIC_COLUMNS, "1"),
}


def test_insert_statement_quotes_strings_not_numbers() -> None:
    sql = insert_statement("fdai_native", [_FULL_ROW])
    assert "'silver_trades'" in sql
    assert "'fast'" in sql
    assert "'2026-08-19 12:00:00.000'" in sql


def test_insert_statement_renders_null_for_empty_cell() -> None:
    row = {**_FULL_ROW, "quarantine_rate_pct": ""}
    sql = insert_statement("fdai_native", [row])
    assert "NULL" in sql


def test_insert_statement_is_one_statement_covering_every_row() -> None:
    sql = insert_statement("fdai_native", [_FULL_ROW, {**_FULL_ROW, "table_name": "gold_bars_1m"}])
    assert len(split_statements(sql)) == 1
    assert sql.count("'silver_trades'") == 1
    assert sql.count("'gold_bars_1m'") == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/awsnative/monitoring/test_collect.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'awsnative.monitoring'`.

- [ ] **Step 3: Create `awsnative/monitoring/__init__.py`**

```python
```

(Empty, matching `awsnative/enrichment/__init__.py`.)

- [ ] **Step 4: Create `awsnative/monitoring/collect.py`**

```python
"""Publish per-table health metrics to CloudWatch and append them to
native_health_metrics.

WHY A LAMBDA HERE, narrowing awsnative/athena.py's own stated preference
("Step Functions calls Athena directly... that is the whole reason stage N2
needs no Lambda"). That reasoning is about the write-critical merge path:
Bronze -> Silver -> Gold, where an extra hop must not add latency or a new
failure mode to data the pipeline exists to produce. CollectHealthMetrics
runs strictly after the merge and its maintenance pass already committed --
it only reads Iceberg metadata and publishes numbers, so a Lambda failing
here cannot corrupt or delay the tables it reports on. In exchange, the
row-to-metric mapping gets real pytest coverage instead of living only in
Step Functions JSON at fixed array positions, where a reordered SELECT
column would silently point a CloudWatch alarm at the wrong number until
someone noticed live.

WHY INSERT, NOT MERGE, into native_health_metrics. This is the only Iceberg
table in this repo with more than one writer: the microbatch, the perp
merge, and the macro merge all append to it. Iceberg's optimistic commit
lock is table-level, not partition-level, so two of those writers landing at
the same moment is a real (if rare) collision -- something no other table
here has had to consider. A MERGE keyed on (table_name, metric_ts) would
still not help, because metric_ts comes from current_timestamp evaluated
inside the query: a retried statement gets a new timestamp, never matches
its own failed attempt, and would insert anyway. Given that, a plain INSERT
is not a shortcut, it is the honest statement of what actually happens on
retry -- and the cost of a rare extra row in an observability table is a
cosmetic blip, not a wrong answer, unlike a duplicated trade or bar would be.
"""

from __future__ import annotations

from typing import Any

_METRIC_COLUMNS = (
    "row_count",
    "file_count",
    "avg_file_size_mb",
    "small_file_pct",
    "delete_file_count",
    "snapshot_count",
    "oldest_snapshot_age_seconds",
    "freshness_lag_seconds",
    "quarantine_rate_pct",
)


def _f(value: str | None) -> float | None:
    """Athena returns every cell as text, and an empty cell means NULL."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _pascal_case(column: str) -> str:
    """row_count -> RowCount, matching spec 2026-08-19 section 4.2's metric names."""
    return "".join(part.capitalize() for part in column.split("_"))


def metric_data(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """CloudWatch PutMetricData entries, one per (table, KPI) that has a value.

    A NULL cell (Athena's empty string) is skipped rather than published as
    zero: "no quarantine rate for this table" and "a measured rate of zero"
    are different facts, and publishing the first as the second would make a
    healthy table indistinguishable from one this metric does not apply to.
    """
    entries: list[dict[str, Any]] = []
    for row in rows:
        table = row["table_name"]
        for column in _METRIC_COLUMNS:
            value = _f(row.get(column))
            if value is None:
                continue
            entries.append(
                {
                    "MetricName": _pascal_case(column),
                    "Dimensions": [{"Name": "TableName", "Value": table}],
                    "Value": value,
                    "Unit": "Seconds" if column.endswith("_seconds") else "None",
                }
            )
    return entries


def _sql_literal(value: str | None, *, quote: bool) -> str:
    """One already-fetched cell, rendered back into a SQL literal.

    Only ever called on values Athena itself just produced from COUNT, AVG,
    or a fixed table-name/tier string -- never on external input -- so a
    plain quote is enough; there is no untrusted text to escape.
    """
    if value in (None, ""):
        return "NULL"
    return f"'{value}'" if quote else value


_INSERT_COLUMNS = ("metric_ts", "table_name", "tier", *_METRIC_COLUMNS)
_STRING_COLUMNS = frozenset({"metric_ts", "table_name", "tier"})


def insert_statement(database: str, rows: list[dict[str, str]]) -> str:
    """One INSERT covering every row already fetched for CloudWatch.

    Built from the values already read back from Athena, rather than
    re-running the SELECT a second time -- current_timestamp evaluated twice,
    moments apart, would make the timestamp CloudWatch published disagree
    with the one stored in history.
    """
    value_rows = [
        "({})".format(
            ", ".join(
                _sql_literal(row.get(column), quote=column in _STRING_COLUMNS)
                for column in _INSERT_COLUMNS
            )
        )
        for row in rows
    ]
    columns_sql = ", ".join(_INSERT_COLUMNS)
    return (
        f"INSERT INTO {database}.native_health_metrics ({columns_sql}) "
        f"VALUES {', '.join(value_rows)}"
    )


def health_metrics_handler(event: dict[str, Any], context: object = None) -> dict[str, str]:
    """Lambda entry point, one call per tail state.

    Event: {"database": "...", "workgroup": "...", "tables": ["silver_trades", ...]}
    """
    from awsnative.athena import AthenaRunner
    from awsnative.render import health_metrics_select_statement

    database = event["database"]
    tables = event["tables"]
    runner = AthenaRunner(database=database, workgroup=event["workgroup"])

    select_sql = health_metrics_select_statement(database, tables)
    outcome = runner.execute(select_sql)
    rows = runner.fetch_rows(outcome.query_execution_id, max_rows=len(tables))

    import boto3

    entries = metric_data(rows)
    if entries:
        # 1000 entries per call is PutMetricData's own limit, comfortably
        # above what 9 KPIs times at most 3 tables per invocation produces.
        boto3.client("cloudwatch").put_metric_data(Namespace="FDAI/Native", MetricData=entries)

    runner.execute(insert_statement(database, rows))
    return {"tables": str(len(rows))}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/awsnative/monitoring/ -v`
Expected: PASS, all 8 tests.

- [ ] **Step 6: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/monitoring/ tests/awsnative/monitoring/
git commit -m "feat(awsnative): add health-metrics Lambda"
```

---

### Task 4: The `native_monitoring` Terraform module

**Files:**
- Create: `infra/modules/native_monitoring/main.tf`
- Create: `infra/modules/native_monitoring/variables.tf`
- Create: `infra/modules/native_monitoring/outputs.tf`

**Interfaces:**
- Consumes: `awsnative/monitoring/*.py` (Task 3), `awsnative/athena.py`, `awsnative/render.py`, `awsnative/sql/**/*.sql` (packaged into the Lambda's zip).
- Produces: outputs `health_metrics_function_arn`, `health_metrics_function_name`, `alert_topic_arn`.

- [ ] **Step 1: Create `infra/modules/native_monitoring/variables.tf`**

```hcl
variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "athena_workgroup_name" {
  type = string
}

variable "source_dir" {
  type        = string
  description = "Absolute path to the repo root, for packaging the health-metrics Lambda."
}

variable "sql_dir" {
  type        = string
  description = <<-EOT
    Absolute path to awsnative/sql. Packaged into the Lambda so
    awsnative/render.py -- which resolves its SQL_DIR relative to its own
    file location -- finds its templates at runtime the same way it does
    when imported outside a Lambda.
  EOT
}

variable "alert_notification_email" {
  type        = string
  description = "Where every CloudWatch alarm in this module sends mail."
}

variable "log_retention_days" {
  type    = number
  default = 7
}
```

- [ ] **Step 2: Create `infra/modules/native_monitoring/main.tf`**

```hcl
# The health-metrics Lambda, and everything that watches its output:
# CloudWatch alarms and the SNS topic they notify. The Lambda itself is
# invoked as a tail state inside native_medallion and native_enrichment's own
# state machines, not scheduled here -- see those modules for the wiring.
locals {
  name = "${var.project}-native-monitoring"

  # CONSTRUCTED rather than taken as module outputs, the same technique
  # native_medallion's own state_machine_arn local already uses: this
  # module's Lambda ARN is an input to native_medallion and
  # native_enrichment (they must invoke it), so taking their state-machine
  # ARNs as inputs here in return would be a circular module dependency.
  microbatch_state_machine_arn  = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-microbatch"
  merge_perp_state_machine_arn  = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-enrichment-merge-perp"
  merge_macro_state_machine_arn = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-enrichment-merge-macro"
}

# --- the Lambda --------------------------------------------------------------

data "archive_file" "package" {
  type        = "zip"
  output_path = "${path.module}/.build/health_metrics.zip"

  source {
    content  = file("${var.source_dir}/awsnative/__init__.py")
    filename = "awsnative/__init__.py"
  }
  source {
    content  = file("${var.source_dir}/awsnative/athena.py")
    filename = "awsnative/athena.py"
  }
  source {
    content  = file("${var.source_dir}/awsnative/render.py")
    filename = "awsnative/render.py"
  }
  dynamic "source" {
    for_each = fileset("${var.source_dir}/awsnative/monitoring", "*.py")
    content {
      content  = file("${var.source_dir}/awsnative/monitoring/${source.value}")
      filename = "awsnative/monitoring/${source.value}"
    }
  }
  # render.py resolves SQL_DIR relative to its own file location at runtime,
  # so the whole sql/ tree has to ship as its sibling in the zip, not just
  # health_metrics_row.sql.
  dynamic "source" {
    for_each = fileset(var.sql_dir, "**/*.sql")
    content {
      content  = file("${var.sql_dir}/${source.value}")
      filename = "awsnative/sql/${source.value}"
    }
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid    = "RunAthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
    ]
    resources = [
      "arn:aws:athena:${var.region}:${var.account_id}:workgroup/${var.athena_workgroup_name}",
      "arn:aws:athena:${var.region}:${var.account_id}:datacatalog/AwsDataCatalog",
    ]
  }

  # Reads every maintained table's metadata (row counts, $files, $snapshots)
  # and commits to native_health_metrics, the only table this role writes.
  statement {
    sid    = "ReadEveryTableWriteHealthMetrics"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:UpdateTable",
      "glue:CreateTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  statement {
    sid    = "ReadWholeLakeWriteHealthMetrics"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [var.lake_bucket_arn, "${var.lake_bucket_arn}/*"]
  }

  statement {
    sid       = "PublishHealthMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource-level scoping.
  }

  statement {
    sid    = "WriteItsOwnLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${local.name}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = local.name
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "health_metrics" {
  function_name    = local.name
  role             = aws_iam_role.lambda.arn
  handler          = "awsnative.monitoring.collect.health_metrics_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256

  # At most 3 tables per invocation, each a handful of small Athena queries
  # plus one SELECT and one INSERT -- generous headroom over the
  # merge/maintenance tail states this runs after, which already fit inside
  # the 5-minute tick.
  timeout     = 180
  memory_size = 256

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# --- alerting: one topic, every alarm notifies it ----------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.project}-native-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_notification_email
}

# Freshness, per table, threshold from DATA_LAYER.md section 7's own SLOs.
# Two consecutive 5-minute periods for the fast tier so one missed tick does
# not alarm; one daily period for the slow tier so its own normal cadence
# does not either.
locals {
  freshness_alarms = {
    silver_trades = {
      threshold          = 360
      period             = 300
      evaluation_periods = 2
    }
    gold_bars_1m = {
      threshold          = 420
      period             = 300
      evaluation_periods = 2
    }
    silver_perp_context = {
      threshold          = 600
      period             = 300
      evaluation_periods = 2
    }
    silver_macro = {
      threshold          = 108000 # 30h: the 24h SLO plus a buffer against the daily poll's own cadence
      period             = 86400
      evaluation_periods = 1
    }
  }

  # "Maintenance stalled": small_file_pct still high two hours after the
  # hourly OPTIMIZE tail state should have run (design 2026-08-17 practice
  # 9 -- alarm on maintenance, not only on the pipeline). Fast tier only:
  # silver_macro never runs OPTIMIZE (design 2026-08-19 section 3.2).
  maintenance_alarms = toset([
    "silver_trades",
    "silver_trades_quarantine",
    "gold_bars_1m",
    "silver_perp_context",
    "native_health_metrics",
  ])

  watched_state_machines = {
    microbatch  = local.microbatch_state_machine_arn
    merge_perp  = local.merge_perp_state_machine_arn
    merge_macro = local.merge_macro_state_machine_arn
  }
}

resource "aws_cloudwatch_metric_alarm" "freshness" {
  for_each = local.freshness_alarms

  alarm_name          = "${var.project}-native-freshness-${each.key}"
  namespace           = "FDAI/Native"
  metric_name         = "FreshnessLagSeconds"
  dimensions          = { TableName = each.key }
  statistic           = "Maximum"
  period              = each.value.period
  evaluation_periods  = each.value.evaluation_periods
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching" # an absent reading means the collection tail state itself stopped
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "quarantine_rate" {
  alarm_name          = "${var.project}-native-quarantine-rate"
  namespace           = "FDAI/Native"
  metric_name         = "QuarantineRatePct"
  dimensions          = { TableName = "silver_trades_quarantine" }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 0.1
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # no quarantined rows yet is not a problem, unlike a stale table
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "maintenance_stalled" {
  for_each = local.maintenance_alarms

  alarm_name          = "${var.project}-native-maintenance-stalled-${each.key}"
  namespace           = "FDAI/Native"
  metric_name         = "SmallFilePct"
  dimensions          = { TableName = each.key }
  statistic           = "Minimum" # the lowest reading in the window must still be high for this to be a real stall
  period              = 300
  evaluation_periods  = 24 # 2 hours at 5-minute readings
  threshold           = 20
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# Free: AWS/States publishes ExecutionsFailed for every state machine with no
# custom code needed. Catches a state machine that stopped entirely, which a
# health-metrics reading cannot: if the writer never ran, it never published
# a stale-but-present reading either.
resource "aws_cloudwatch_metric_alarm" "execution_failed" {
  for_each = local.watched_state_machines

  alarm_name          = "${var.project}-native-executions-failed-${each.key}"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = each.value }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # no executions in the window is not a failure
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

- [ ] **Step 3: Create `infra/modules/native_monitoring/outputs.tf`**

```hcl
output "health_metrics_function_arn" {
  value = aws_lambda_function.health_metrics.arn
}

output "health_metrics_function_name" {
  value = aws_lambda_function.health_metrics.function_name
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
```

- [ ] **Step 4: Validate offline**

This module is not yet wired into `infra/envs/native` (Task 5 does that), so `terraform validate` cannot run against it standalone. Confirm syntax only:

Run: `terraform fmt -recursive infra/modules/native_monitoring && terraform -chdir=infra/modules/native_monitoring init -backend=false -input=false && terraform -chdir=infra/modules/native_monitoring validate`

Expected: PASS. Full validation, including `native_render_parity.sh`, happens in Task 5 once this module is wired in.

- [ ] **Step 5: Commit**

```bash
git add infra/modules/native_monitoring/
git commit -m "feat(infra): add native_monitoring module (Lambda, SNS, CloudWatch alarms)"
```

---

### Task 5: Wire `native_monitoring` into `infra/envs/native`

**Files:**
- Modify: `infra/envs/native/main.tf`
- Modify: `infra/envs/native/variables.tf`
- Modify: `infra/envs/native/outputs.tf`

**Interfaces:**
- Consumes: `native_monitoring`'s `health_metrics_function_arn`, `alert_topic_arn` outputs (Task 4).
- Produces: `module.monitoring.health_metrics_function_arn`, referenced by Task 6 and Task 7's `module "medallion"`/`module "enrichment"` blocks.

- [ ] **Step 1: Add the notification email variable**

In `infra/envs/native/variables.tf`, add, directly after the existing `budget_notification_email` variable:

```hcl
variable "alert_notification_email" {
  description = "Where CloudWatch alarms for the AWS-native stack send mail."
  type        = string
}
```

- [ ] **Step 2: Add the `monitoring` module block**

In `infra/envs/native/main.tf`, add a new module block directly after `module "lakehouse"` (before `module "stream"`):

```hcl
# Spec 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4. Placed
# ahead of "medallion" and "enrichment" below because both need this
# module's Lambda ARN to invoke it as a tail state; this module needs
# nothing from either in return (see native_monitoring/main.tf's comment on
# why its state-machine ARNs are constructed, not referenced).
module "monitoring" {
  source = "../../modules/native_monitoring"

  project    = var.project
  region     = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id

  lake_bucket_arn        = module.lakehouse.bucket_arn
  glue_database_name     = module.lakehouse.glue_database_name
  athena_workgroup_name  = module.lakehouse.athena_workgroup_name
  source_dir             = abspath("${path.root}/../../..")
  sql_dir                = abspath("${path.root}/../../../awsnative/sql")

  alert_notification_email = var.alert_notification_email
}
```

- [ ] **Step 3: Pass the Lambda ARN into `medallion` and `enrichment`**

In the same file, change:

```hcl
module "medallion" {
  source     = "../../modules/native_medallion"
  project    = var.project
  region     = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id

  lake_bucket_arn       = module.lakehouse.bucket_arn
  glue_database_name    = module.lakehouse.glue_database_name
  athena_workgroup_name = module.lakehouse.athena_workgroup_name
```

to:

```hcl
module "medallion" {
  source     = "../../modules/native_medallion"
  project    = var.project
  region     = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id

  lake_bucket_arn             = module.lakehouse.bucket_arn
  glue_database_name          = module.lakehouse.glue_database_name
  athena_workgroup_name       = module.lakehouse.athena_workgroup_name
  health_metrics_function_arn = module.monitoring.health_metrics_function_arn
```

and change:

```hcl
module "enrichment" {
  source                = "../../modules/native_enrichment"
  project               = var.project
  region                = var.region
  account_id            = data.aws_caller_identity.current.account_id
  lake_bucket_name      = module.lakehouse.bucket_name
  lake_bucket_arn       = module.lakehouse.bucket_arn
  glue_database_name    = module.lakehouse.glue_database_name
  athena_workgroup_name = module.lakehouse.athena_workgroup_name
  source_dir            = abspath("${path.root}/../../..")
```

to:

```hcl
module "enrichment" {
  source                      = "../../modules/native_enrichment"
  project                     = var.project
  region                      = var.region
  account_id                  = data.aws_caller_identity.current.account_id
  lake_bucket_name            = module.lakehouse.bucket_name
  lake_bucket_arn             = module.lakehouse.bucket_arn
  glue_database_name          = module.lakehouse.glue_database_name
  athena_workgroup_name       = module.lakehouse.athena_workgroup_name
  health_metrics_function_arn = module.monitoring.health_metrics_function_arn
  source_dir                  = abspath("${path.root}/../../..")
```

- [ ] **Step 4: Add outputs for the alert topic**

In `infra/envs/native/outputs.tf`, add:

```hcl
output "alert_topic_arn" {
  value = module.monitoring.alert_topic_arn
}

output "health_metrics_function" {
  value = module.monitoring.health_metrics_function_name
}
```

- [ ] **Step 5: Validate**

Run: `terraform fmt -recursive infra/envs/native infra/modules/native_monitoring`

This will fail `terraform validate` until Task 6 and Task 7 add `health_metrics_function_arn` as a declared variable on `native_medallion` and `native_enrichment` — Terraform rejects an undeclared input variable at `validate` time. Do not attempt to make this task's validate step pass in isolation; proceed to Task 6.

- [ ] **Step 6: Commit**

```bash
git add infra/envs/native/main.tf infra/envs/native/variables.tf infra/envs/native/outputs.tf
git commit -m "feat(infra): wire native_monitoring into the native environment"
```

---

### Task 6: `CollectHealthMetrics` in `fdai-native-microbatch`

**Files:**
- Modify: `infra/modules/native_medallion/variables.tf`
- Modify: `infra/modules/native_medallion/main.tf`

**Interfaces:**
- Consumes: `var.health_metrics_function_arn` (new), Plan 1's `local.optimize_sql`/`local.vacuum_sql` maps (extended here to include `native_health_metrics`).
- Produces: rewired terminal states — `CollectHealthMetrics` becomes the new end of the execution, replacing the `MaintenanceDone` Succeed state Plan 1 added.

- [ ] **Step 1: Add the new variable**

In `infra/modules/native_medallion/variables.tf`, add:

```hcl
variable "health_metrics_function_arn" {
  type        = string
  description = "ARN of the health-metrics Lambda (native_monitoring module), invoked as this state machine's final tail state."
}
```

- [ ] **Step 2: Extend the maintenance locals to cover `native_health_metrics`**

In `infra/modules/native_medallion/main.tf`, change the `maintenance_predicates` map Plan 1 added:

```hcl
  maintenance_predicates = {
    silver_trades            = "event_ts >= current_date - interval '${var.lookback_days}' day"
    silver_trades_quarantine = "ingest_date >= date_format(current_date - interval '${var.lookback_days}' day, '%Y-%m-%d')"
    gold_bars_1m             = "window_end_ts >= current_date - interval '${var.lookback_days}' day"
  }
```

to:

```hcl
  maintenance_predicates = {
    silver_trades            = "event_ts >= current_date - interval '${var.lookback_days}' day"
    silver_trades_quarantine = "ingest_date >= date_format(current_date - interval '${var.lookback_days}' day, '%Y-%m-%d')"
    gold_bars_1m             = "window_end_ts >= current_date - interval '${var.lookback_days}' day"
    # Written by three state machines, so this module is the sole
    # maintainer of native_health_metrics -- neither merge_perp nor
    # merge_macro runs OPTIMIZE/VACUUM against it (spec
    # 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4.3).
    native_health_metrics = "metric_ts >= current_date - interval '${var.lookback_days}' day"
  }
```

(`optimize_sql` and `vacuum_sql`, defined as `for` expressions over the keys of `maintenance_predicates`, pick up the new entry automatically — no further change needed to those two locals.)

- [ ] **Step 3: Add the metrics-table maintenance states and rewire the terminal states**

Change:

```hcl
      OptimizeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "IsTopOfDay"
      }

      # VACUUM runs daily regardless of which hour triggered OPTIMIZE above:
      # expiry and orphan removal address storage, the slow-moving problem,
      # while OPTIMIZE addresses reads, the fast one (spec 2026-08-17,
      # section 8.2).
      IsTopOfDay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.hour"
          StringEquals = "00"
          Next         = "VacuumSilverTrades"
        }]
        Default = "MaintenanceDone"
      }
```

to:

```hcl
      OptimizeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeNativeHealthMetrics"
      }

      OptimizeNativeHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["native_health_metrics"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "IsTopOfDay"
      }

      # VACUUM runs daily regardless of which hour triggered OPTIMIZE above:
      # expiry and orphan removal address storage, the slow-moving problem,
      # while OPTIMIZE addresses reads, the fast one (spec 2026-08-17,
      # section 8.2). Every path -- whether or not this is the top of the
      # hour or the day -- converges on CollectHealthMetrics: a tick that
      # skips maintenance must not also skip reporting on the tables.
      IsTopOfDay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.hour"
          StringEquals = "00"
          Next         = "VacuumSilverTrades"
        }]
        Default = "CollectHealthMetrics"
      }
```

Then change:

```hcl
      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverTrades"
        }]
        Default = "MaintenanceDone"
      }
```

to:

```hcl
      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverTrades"
        }]
        Default = "CollectHealthMetrics"
      }
```

Then change:

```hcl
      VacuumGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "MaintenanceDone"
      }

      MaintenanceDone = {
        Type = "Succeed"
      }
    }
  })
}
```

to:

```hcl
      VacuumGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumNativeHealthMetrics"
      }

      VacuumNativeHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["native_health_metrics"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "CollectHealthMetrics"
      }

      # Replaces Plan 1's MaintenanceDone Succeed state: every path through
      # maintenance, run or skipped, ends here. Monitoring section 4.1.
      CollectHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.health_metrics_function_arn
          Payload = {
            database  = var.glue_database_name
            workgroup = var.athena_workgroup_name
            tables    = ["silver_trades", "silver_trades_quarantine", "gold_bars_1m"]
          }
        }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        TimeoutSeconds = 200
        End            = true
      }
    }
  })
}
```

- [ ] **Step 4: Add the Lambda-invoke IAM permission**

In the same file, inside `data "aws_iam_policy_document" "sfn_permissions"`, add a new statement (placement does not matter; add it directly after the existing `SeeItsOwnExecutions` statement):

```hcl
  statement {
    sid       = "InvokeHealthMetricsLambda"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [var.health_metrics_function_arn]
  }
```

- [ ] **Step 5: Validate offline**

Run: `make validate-aws`
Expected: `terraform validate` and `terraform fmt -check` pass across `infra/envs/native` and every module (this is the first point where Task 5's env-level wiring type-checks end to end, since `native_medallion` now declares the variable Task 5 already passes into it); `scripts/native_render_parity.sh` passes for every Terraform-rendered `.sql` file (it does not check `health_metrics_row.sql`, which only the Lambda renders — see this plan's Task 2).

- [ ] **Step 6: Commit**

```bash
git add infra/modules/native_medallion/variables.tf infra/modules/native_medallion/main.tf
git commit -m "feat(infra): add CollectHealthMetrics tail state to fdai-native-microbatch"
```

---

### Task 7: `CollectHealthMetrics` in `fdai-native-enrichment-merge-perp`

**Files:**
- Modify: `infra/modules/native_enrichment/variables.tf`
- Modify: `infra/modules/native_enrichment/main.tf`

**Interfaces:**
- Consumes: `var.health_metrics_function_arn` (new, shared by Task 8 since both state machines live in this one file/module).
- Produces: rewired terminal state in `aws_sfn_state_machine.merge_perp`.

- [ ] **Step 1: Add the new variable**

In `infra/modules/native_enrichment/variables.tf`, add:

```hcl
variable "health_metrics_function_arn" {
  type        = string
  description = "ARN of the health-metrics Lambda (native_monitoring module), invoked as a tail state in both merge state machines."
}
```

- [ ] **Step 2: Add the Lambda-invoke IAM permission**

In `infra/modules/native_enrichment/main.tf`, inside `data "aws_iam_policy_document" "merge_sfn_permissions"`, add a new statement directly after the existing `SeeItsOwnExecutions` statement:

```hcl
  statement {
    sid       = "InvokeHealthMetricsLambda"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [var.health_metrics_function_arn]
  }
```

(This one grant on `merge_sfn` covers both `merge_perp` and `merge_macro`, since Plan 1's `aws_iam_role.merge_sfn` is already the single role both state machines share.)

- [ ] **Step 3: Rewire `merge_perp`'s terminal states**

Change:

```hcl
      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverPerpContext"
        }]
        Default = "MaintenanceDone"
      }
```

to:

```hcl
      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverPerpContext"
        }]
        Default = "CollectHealthMetrics"
      }
```

Change:

```hcl
      IsTopOfDay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.hour"
          StringEquals = "00"
          Next         = "VacuumSilverPerpContext"
        }]
        Default = "MaintenanceDone"
      }

      VacuumSilverPerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_perp_context"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "MaintenanceDone"
      }

      MaintenanceDone = {
        Type = "Succeed"
      }
    }
  })
}
```

to:

```hcl
      IsTopOfDay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.hour"
          StringEquals = "00"
          Next         = "VacuumSilverPerpContext"
        }]
        Default = "CollectHealthMetrics"
      }

      VacuumSilverPerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_perp_context"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "CollectHealthMetrics"
      }

      # Replaces Plan 1's MaintenanceDone Succeed state.
      CollectHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.health_metrics_function_arn
          Payload = {
            database  = var.glue_database_name
            workgroup = var.athena_workgroup_name
            tables    = ["silver_perp_context"]
          }
        }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        TimeoutSeconds = 200
        End            = true
      }
    }
  })
}
```

- [ ] **Step 4: Validate offline**

Run: `make validate-aws`
Expected: PASS, across both modules and the env.

- [ ] **Step 5: Commit**

```bash
git add infra/modules/native_enrichment/variables.tf infra/modules/native_enrichment/main.tf
git commit -m "feat(infra): add CollectHealthMetrics tail state to fdai-native-enrichment-merge-perp"
```

---

### Task 8: `CollectHealthMetrics` in `fdai-native-enrichment-merge-macro`

**Files:**
- Modify: `infra/modules/native_enrichment/main.tf`

**Interfaces:**
- Consumes: `var.health_metrics_function_arn` (Task 7 already declared it on this module).
- Produces: rewired terminal state in `aws_sfn_state_machine.merge_macro`.

- [ ] **Step 1: Rewire `merge_macro`'s terminal state**

Change:

```hcl
      # No hour/minute gate: this state machine already runs once a day, on
      # the same schedule as the macro poll (design 2026-08-19 section 3.2),
      # so one VACUUM per execution already is the daily cadence.
      VacuumSilverMacro = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_macro"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        End            = true
      }
    }
  })
}
```

to:

```hcl
      # No hour/minute gate: this state machine already runs once a day, on
      # the same schedule as the macro poll (design 2026-08-19 section 3.2),
      # so one VACUUM per execution already is the daily cadence.
      VacuumSilverMacro = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_macro"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "CollectHealthMetrics"
      }

      CollectHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.health_metrics_function_arn
          Payload = {
            database  = var.glue_database_name
            workgroup = var.athena_workgroup_name
            tables    = ["silver_macro"]
          }
        }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        TimeoutSeconds = 200
        End            = true
      }
    }
  })
}
```

- [ ] **Step 2: Validate offline**

Run: `make validate-aws`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add infra/modules/native_enrichment/main.tf
git commit -m "feat(infra): add CollectHealthMetrics tail state to fdai-native-enrichment-merge-macro"
```

---

### Task 9: Deploy, verify against a live account, update docs

**Files:**
- Modify: `docs/DATA_LAYER_NEXT_STEPS.md` (§3.4)

No offline test substitutes for this task — it is the live-account verification the earlier tasks' `terraform validate` cannot cover (Lambda IAM correctness, whether M2's `content` column exists, whether the SNS subscription confirms, whether CloudWatch actually receives the metrics).

- [ ] **Step 1: Deploy**

```bash
terraform -chdir=infra/envs/native apply
```

Review the plan before confirming. Expect: one new Lambda, one new IAM role/policy, one SNS topic and subscription, ~11 CloudWatch alarms, and modified (not recreated) state machines in `native_medallion` and `native_enrichment`. A Step Functions state machine's `definition` changing is an in-place update, not a replacement — if Terraform proposes replacing either state machine, stop and investigate before confirming, since that is not what this plan's diffs should cause.

- [ ] **Step 2: Confirm the SNS subscription**

Check the inbox for `var.alert_notification_email` and click the AWS confirmation link. Alarms will not notify until this is done — `aws sns list-subscriptions-by-topic --topic-arn "$(terraform -chdir=infra/envs/native output -raw alert_topic_arn)"` shows `PendingConfirmation` until then.

- [ ] **Step 3: Trigger each state machine once and confirm `CollectHealthMetrics` ran**

```bash
make microbatch-aws
make merge-enrich-aws
```

Both block until `SUCCEEDED`. If either fails at the `CollectHealthMetrics` state specifically (not at a merge or maintenance state), check the Lambda's own logs:

```bash
aws logs tail "/aws/lambda/$(terraform -chdir=infra/envs/native output -raw health_metrics_function)" --since 10m
```

- [ ] **Step 4: Verify M2 — does `$files` expose a `content` column**

```bash
uv run --group awsnative python -m awsnative.query \
  --database "$(terraform -chdir=infra/envs/native output -raw glue_database)" \
  --workgroup "$(terraform -chdir=infra/envs/native output -raw athena_workgroup)" \
  --sql 'DESCRIBE "silver_trades$files"'
```

If `content` is not a column, M2 is false. Apply the named fallback: in `awsnative/sql/health_metrics_row.sql`, replace the `delete_file_count` subquery with `CAST(NULL AS bigint) AS delete_file_count`, re-run Task 2's tests (the `test_health_metrics_select_*` tests do not assert on `delete_file_count`'s content, so they should still pass unchanged), and redeploy.

- [ ] **Step 5: Confirm CloudWatch received the metrics**

```bash
aws cloudwatch list-metrics --namespace FDAI/Native
```

Expected: entries for `RowCount`, `FreshnessLagSeconds`, `SmallFilePct`, etc., each dimensioned by `TableName`, for every table in `render.MAINTAINED_TABLES` except `native_health_metrics` (which is maintained but not itself monitored — spec 2026-08-19 section 4.3 and this plan's Global Constraints).

- [ ] **Step 6: Confirm the alarms are not stuck in `INSUFFICIENT_DATA`**

```bash
aws cloudwatch describe-alarms --alarm-name-prefix "fdai-native-" --query 'MetricAlarms[].[AlarmName,StateValue]' --output table
```

An alarm needs one full `period` × `evaluation_periods` of data before it leaves `INSUFFICIENT_DATA` — for the fast-tier freshness alarms that is 10 minutes after the first successful `CollectHealthMetrics` run; for `silver_macro`'s alarm and the maintenance-stalled alarms, it is longer (a day, and 2 hours, respectively). Re-check after that much time has passed rather than treating `INSUFFICIENT_DATA` at the 5-minute mark as a failure.

- [ ] **Step 7: Update `docs/DATA_LAYER_NEXT_STEPS.md` §3.4**

Change the section heading from `### 3.4 Alerting on the checks that already exist` to `### 3.4 Alerting — running` and replace the paragraph body with:

```markdown
Implemented: [`2026-08-20-iceberg-health-metrics-monitoring.md`](../superpowers/plans/2026-08-20-iceberg-health-metrics-monitoring.md)
publishes freshness, quarantine rate, and file/delete-file/snapshot counts to
CloudWatch (namespace `FDAI/Native`) from a new tail state in each writer
state machine, with alarms on freshness, quarantine rate, and stalled
maintenance, all notifying one SNS topic. History lives in the new
`native_health_metrics` table, which [`2026-08-19-iceberg-housekeeping-monitoring-design.md`](../superpowers/specs/2026-08-19-iceberg-housekeeping-monitoring-design.md)
section 5's QuickSight dashboard reads from — that plan has not been written
yet.
```

- [ ] **Step 8: Commit the doc update**

```bash
git add docs/DATA_LAYER_NEXT_STEPS.md
git commit -m "docs: record health-metrics monitoring as implemented, not proposed"
```

---

## Self-Review Notes

- **Spec coverage**: section 4.1 (tail state, two destinations) → Tasks 3, 6, 7, 8; section 4.2 (CloudWatch metrics, namespace/dimensions) → Task 3's `metric_data()`; section 4.3 (`native_health_metrics` schema and its own maintenance) → Tasks 1, 6; section 4.4 (alarm table) → Task 4; section 4.5 (IAM, additive only) → Tasks 4, 6, 7.
- **Deviations from the spec recorded, not silently made**: the spec's schema sketch listed a stored `metric_date` column (Task 1 uses the partition transform instead, no stored column); the spec described the tail state's mechanism in terms of what happens, which this plan implements as a Lambda after an explicit trade-off discussion recorded in the Global Constraints and in `collect.py`'s own docstring, narrowing `awsnative/athena.py`'s stated no-Lambda-in-the-merge-path principle; the spec's MERGE-based dedup idea for `native_health_metrics` is replaced with a plain INSERT, with the reasoning for why that is still safe recorded in the same docstring.
- **Circular dependency identified and resolved before writing Terraform**, not discovered mid-implementation: `native_monitoring` constructs state-machine ARNs rather than taking them as module outputs, exactly mirroring the technique `native_medallion` already uses for its own overlap guard.
- **M2 is still open**: Task 9 verifies it live and states the exact fallback, rather than the plan silently assuming Athena's Iceberg `$files` metadata table exposes a `content` column.
