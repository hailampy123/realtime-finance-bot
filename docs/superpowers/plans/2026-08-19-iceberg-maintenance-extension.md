# Iceberg Maintenance Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Athena `OPTIMIZE` and `VACUUM` against all six Iceberg tables in the AWS-native lake, as tail states of the state machines that already write them, so small-file and delete-file growth stops being unmanaged.

**Architecture:** Generic, parameterized `OPTIMIZE`/`VACUUM` SQL templates rendered once per table (mirroring `awsnative/render.py`'s existing merge-statement pattern), wired into the existing state machines (`fdai-native-microbatch`, `fdai-native-enrichment-merge-perp`, `fdai-native-enrichment-merge-macro`) as tail states behind execution-time `Choice` gates. One state machine stays one writer — no new schedule, no new state machine, no new IAM role.

**Tech Stack:** Terraform (AWS provider), Trino/Athena SQL, Python 3.12 (`string.Template` rendering, pytest).

**Spec:** [`docs/superpowers/specs/2026-08-19-iceberg-housekeeping-monitoring-design.md`](../specs/2026-08-19-iceberg-housekeeping-monitoring-design.md), sections 1–3. This plan implements those sections only — not monitoring (section 4) or the dashboard (section 5), which get their own plans once this one lands.

## Global Constraints

- No new IAM role, policy widening, or schedule. Verified against the current policies: `native_medallion`'s Glue grant is already database-wide (`table/${var.glue_database_name}/*`) and its S3 grant already covers the whole bucket including `DeleteObject`; `native_enrichment`'s `merge_sfn` role already names `silver_perp_context` and `silver_macro` explicitly with `DeleteObject` on their prefixes. If any task in this plan discovers a permission is actually missing, stop and re-verify against the live `aws_iam_role_policy` resource before adding one — the design's own recommendation (spec 2026-08-17, section 8.1) depends on this being true.
- Every `.sql` file added under `awsnative/sql/` must use only placeholders already in (or added to) `render.KNOWN_PLACEHOLDERS` — `tests/awsnative/test_render.py::test_only_known_placeholders` enforces this automatically over every file the glob finds, so no test change is needed there.
- Terraform and Python render the same `.sql` files independently (`templatefile()` vs `string.Template`). `scripts/native_render_parity.sh` checks they produce byte-identical output — run it (via `make validate-aws`) after every Terraform change in this plan.
- `silver_macro` gets `VACUUM` only, never `OPTIMIZE`: it takes about one commit a day (`docs/DATA_LAYER.md` §8) and has no timestamp column to window a partition predicate on (`awsnative/sql/ddl/053_silver_macro.sql` — only `observation_date` and `vintage_date`, both `date`, and the table is partitioned by `series_id` alone).
- `OPTIMIZE`'s `WHERE` clause accepts partition columns only (spec 2026-08-17, assumption M1) — Task 7 verifies this against a live account before treating the design as confirmed.

---

### Task 1: Generic maintenance SQL templates and their Python renderer

**Files:**
- Create: `awsnative/sql/optimize_table.sql`
- Create: `awsnative/sql/vacuum_table.sql`
- Modify: `awsnative/render.py` — add two placeholders to `KNOWN_PLACEHOLDERS`, append a new section after `enrichment_statements()`
- Test: `tests/awsnative/test_render.py`

**Interfaces:**
- Produces: `render.OPTIMIZE_TABLE: str`, `render.VACUUM_TABLE: str` (filenames), `render.MAINTENANCE_PARTITION_PREDICATES: dict[str, str]`, `render.MAINTAINED_TABLES: tuple[str, ...]`, `render.maintenance_statements(database: str, lookback_days: int = 1) -> dict[str, dict[str, str | None]]` — keyed by table name, each value `{"vacuum": <sql>, "optimize": <sql or None>}`. Tasks 3–5 (Terraform) render the same two `.sql` files independently via `templatefile()`; they do not call this Python function, but must produce output that matches it, which `native_render_parity.sh` checks.

- [ ] **Step 1: Write the failing tests**

Append to `tests/awsnative/test_render.py`:

```python
def test_maintenance_statements_cover_every_maintained_table() -> None:
    statements = render.maintenance_statements("fdai_native")
    assert set(statements) == set(render.MAINTAINED_TABLES)


def test_silver_macro_gets_vacuum_only() -> None:
    """One commit a day and no timestamp column to window on -- OPTIMIZE would
    never find anything to rewrite (design 2026-08-19 section 3.2)."""
    statements = render.maintenance_statements("fdai_native")
    assert statements["silver_macro"]["optimize"] is None
    assert statements["silver_macro"]["vacuum"] is not None


def test_maintenance_statements_are_single_statements_with_no_placeholder_left() -> None:
    from awsnative.athena import split_statements

    statements = render.maintenance_statements("fdai_native", lookback_days=2)
    for table, kinds in statements.items():
        for kind, sql in kinds.items():
            if sql is None:
                continue
            assert len(split_statements(sql)) == 1, f"{table} {kind} rendered to more than one statement"
            assert "${" not in sql, f"{table} {kind} still has an unrendered placeholder"


def test_optimize_predicates_reference_the_tables_own_partition_column() -> None:
    statements = render.maintenance_statements("fdai_native")
    assert "event_ts" in statements["silver_trades"]["optimize"]
    assert "ingest_date" in statements["silver_trades_quarantine"]["optimize"]
    assert "window_end_ts" in statements["gold_bars_1m"]["optimize"]
    assert "snapshot_ts" in statements["silver_perp_context"]["optimize"]


def test_vacuum_statement_has_no_where_clause() -> None:
    """Retention comes from table properties, not a predicate (design 2026-08-17 section 8.3)."""
    statements = render.maintenance_statements("fdai_native")
    for table in render.MAINTAINED_TABLES:
        assert "WHERE" not in statements[table]["vacuum"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/awsnative/test_render.py -k maintenance -v`
Expected: FAIL with `AttributeError: module 'awsnative.render' has no attribute 'maintenance_statements'` (or similar, for the other new names).

- [ ] **Step 3: Create the two SQL templates**

`awsnative/sql/optimize_table.sql`:

```sql
-- Generic bin-pack compaction for one Iceberg table, gated to a partition
-- window so a pass never re-scans the whole table (spec
-- 2026-08-17-iceberg-table-maintenance-design.md section 8.2).
--
-- partition_predicate is a caller-supplied fragment, not a bare column
-- name: each table's WHERE clause differs by column name and comparison
-- shape (spec 2026-08-19-iceberg-housekeeping-monitoring-design.md section
-- 3.1). The caller renders the predicate first and splices it in -- the same
-- two-step composition fragments/dirty_from_bronze.sql uses.
OPTIMIZE ${database}.${table} REWRITE DATA USING BIN_PACK WHERE ${partition_predicate}
```

`awsnative/sql/vacuum_table.sql`:

```sql
-- Generic snapshot expiry and orphan-file removal for one Iceberg table.
-- No WHERE clause: retention is controlled by the table's own
-- vacuum_max_snapshot_age_seconds and vacuum_min_snapshots_to_keep
-- properties (spec 2026-08-17-iceberg-table-maintenance-design.md section
-- 8.3), set in the table's DDL rather than passed here.
VACUUM ${database}.${table}
```

- [ ] **Step 4: Add the two placeholders to `KNOWN_PLACEHOLDERS`**

In `awsnative/render.py`, change:

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
    }
)
```

- [ ] **Step 5: Append the maintenance renderer**

Append to the end of `awsnative/render.py` (after `enrichment_statements()`):

```python
# --- maintenance: OPTIMIZE and VACUUM, spec 2026-08-19 section 3 -----------
#
# One partition predicate per table, as a ${lookback_days}-templated
# fragment -- rendered in two steps, same composition pattern as
# dirty_from_bronze(): the predicate is filled in first, then spliced into
# optimize_table.sql as a single opaque value. A table with no entry here
# gets VACUUM only.
OPTIMIZE_TABLE = "optimize_table.sql"
VACUUM_TABLE = "vacuum_table.sql"

MAINTENANCE_PARTITION_PREDICATES: dict[str, str] = {
    "silver_trades": "event_ts >= current_date - interval '${lookback_days}' day",
    "silver_trades_quarantine": (
        "ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')"
    ),
    "gold_bars_1m": "window_end_ts >= current_date - interval '${lookback_days}' day",
    "silver_perp_context": "snapshot_ts >= current_date - interval '${lookback_days}' day",
}

# Every Iceberg table under maintenance. silver_macro has no entry in
# MAINTENANCE_PARTITION_PREDICATES above, so maintenance_statements() below
# gives it "optimize": None -- it takes about one commit a day and has no
# timestamp column to window a predicate on.
MAINTAINED_TABLES = (
    "silver_trades",
    "silver_trades_quarantine",
    "gold_bars_1m",
    "silver_perp_context",
    "silver_macro",
)


def maintenance_statements(
    database: str, lookback_days: int = 1
) -> dict[str, dict[str, str | None]]:
    """{table: {"vacuum": sql, "optimize": sql | None}} for every maintained table.

    Terraform renders the same optimize_table.sql/vacuum_table.sql with the
    same predicates for the maintenance tail states in native_medallion and
    native_enrichment. This function exists for tests and for running a pass
    by hand; it is not what production executes.
    """
    vacuum_template = read_sql(VACUUM_TABLE)
    optimize_template = read_sql(OPTIMIZE_TABLE)
    result: dict[str, dict[str, str | None]] = {}
    for table in MAINTAINED_TABLES:
        vacuum_sql = render(vacuum_template, database=database, table=table)
        predicate_template = MAINTENANCE_PARTITION_PREDICATES.get(table)
        optimize_sql = None
        if predicate_template is not None:
            predicate = render(predicate_template, lookback_days=lookback_days)
            optimize_sql = render(
                optimize_template,
                database=database,
                table=table,
                partition_predicate=predicate,
            )
        result[table] = {"vacuum": vacuum_sql, "optimize": optimize_sql}
    return result
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/awsnative/test_render.py -v`
Expected: PASS, including the pre-existing tests (the new files must not break `test_there_are_sql_files_to_check`'s `>= 8` floor, and `test_only_known_placeholders`/`test_no_terraform_directive_syntax`/`test_no_placeholder_inside_a_line_comment` must pass on the two new files automatically since they parametrize over every file under `render.SQL_DIR`).

- [ ] **Step 7: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/sql/optimize_table.sql awsnative/sql/vacuum_table.sql awsnative/render.py tests/awsnative/test_render.py
git commit -m "feat(awsnative): add generic OPTIMIZE/VACUUM SQL templates and renderer"
```

---

### Task 2: Vacuum retention properties on the five maintained tables' DDL

**Files:**
- Modify: `awsnative/sql/ddl/010_silver_trades.sql`
- Modify: `awsnative/sql/ddl/020_silver_trades_quarantine.sql`
- Modify: `awsnative/sql/ddl/030_gold_bars_1m.sql`
- Modify: `awsnative/sql/ddl/052_silver_perp_context.sql`
- Modify: `awsnative/sql/ddl/053_silver_macro.sql`
- Test: `tests/awsnative/test_sql_contracts.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: no new names — this only adds two table properties so `make ddl-aws` creates a configured table and no `ALTER TABLE` step is ever needed (spec 2026-08-17, section 8.3).

- [ ] **Step 1: Write the failing test**

Append to `tests/awsnative/test_sql_contracts.py`:

```python
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
)


def test_maintained_tables_set_vacuum_retention_properties() -> None:
    for name in MAINTAINED_DDL:
        assert "'vacuum_max_snapshot_age_seconds' = '3600'" in DDL[name], name
        assert "'vacuum_min_snapshots_to_keep'    = '5'" in DDL[name], name
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/awsnative/test_sql_contracts.py -k vacuum_retention -v`
Expected: FAIL, `AssertionError` on the first table in the tuple.

- [ ] **Step 3: Edit each DDL file's `TBLPROPERTIES` block**

In each of the five files, change:

```sql
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
```

to:

```sql
TBLPROPERTIES (
    'table_type'                      = 'ICEBERG',
    'format'                          = 'parquet',
    'write_compression'               = 'snappy',
    'vacuum_max_snapshot_age_seconds' = '3600',
    'vacuum_min_snapshots_to_keep'    = '5'
)
```

(Alignment of the `=` column shifts because the new keys are longer — match the existing file's style of aligning every `=` in the block.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/awsnative/test_sql_contracts.py -v`
Expected: PASS, all tests in the file including the pre-existing ones (they read the same `DDL` dict, so a syntax mistake in any of the five files fails `test_every_ddl_file_is_classified` or `test_every_merge_target_is_iceberg` too, not just the new test).

- [ ] **Step 5: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/sql/ddl/010_silver_trades.sql awsnative/sql/ddl/020_silver_trades_quarantine.sql awsnative/sql/ddl/030_gold_bars_1m.sql awsnative/sql/ddl/052_silver_perp_context.sql awsnative/sql/ddl/053_silver_macro.sql tests/awsnative/test_sql_contracts.py
git commit -m "feat(awsnative): set vacuum retention properties on maintained tables"
```

---

### Task 3: Maintenance tail states in `fdai-native-microbatch`

**Files:**
- Modify: `infra/modules/native_medallion/main.tf`

**Interfaces:**
- Consumes: `render.OPTIMIZE_TABLE`/`render.VACUUM_TABLE` filenames from Task 1 (via `templatefile()`, not the Python function), `var.sql_dir`, `var.glue_database_name`, `var.athena_workgroup_name`, `var.lookback_days`, `var.query_timeout_seconds`, `local.athena_retry` — all already defined in this file.
- Produces: new Terraform locals `local.maintenance_predicates`, `local.optimize_sql` (map keyed by table name), `local.vacuum_sql` (same); new ASL states `DeriveMaintenanceClock`, `IsTopOfHour`, `OptimizeSilverTrades`, `OptimizeQuarantine`, `OptimizeGold`, `IsTopOfDay`, `VacuumSilverTrades`, `VacuumQuarantine`, `VacuumGold`, `MaintenanceDone` inside `aws_sfn_state_machine.microbatch`.

- [ ] **Step 1: Add the maintenance locals**

In `infra/modules/native_medallion/main.tf`, inside the existing `locals { ... }` block, after the `athena_retry` entry (before the closing `}` of the locals block), add:

```hcl
  # --- maintenance SQL, spec 2026-08-19 section 3 ---------------------------
  #
  # One partition predicate per table, matching each table's own partition
  # column (spec 2026-08-17 section 8.2). Rendered independently from
  # awsnative/render.py's maintenance_statements(), which the same two files
  # feed for tests; scripts/native_render_parity.sh checks the two agree.
  maintenance_predicates = {
    silver_trades            = "event_ts >= current_date - interval '${var.lookback_days}' day"
    silver_trades_quarantine = "ingest_date >= date_format(current_date - interval '${var.lookback_days}' day, '%Y-%m-%d')"
    gold_bars_1m             = "window_end_ts >= current_date - interval '${var.lookback_days}' day"
  }

  optimize_sql = {
    for table, predicate in local.maintenance_predicates : table => templatefile("${var.sql_dir}/optimize_table.sql", {
      database            = var.glue_database_name
      table               = table
      partition_predicate = predicate
    })
  }

  vacuum_sql = {
    for table in keys(local.maintenance_predicates) : table => templatefile("${var.sql_dir}/vacuum_table.sql", {
      database = var.glue_database_name
      table    = table
    })
  }
```

- [ ] **Step 2: Change `MergeGold` to hand off to maintenance**

In the same file, inside `aws_sfn_state_machine.microbatch`'s `definition`, change:

```hcl
      MergeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_gold
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
      MergeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_gold
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "DeriveMaintenanceClock"
      }

      # Housekeeping tail, spec 2026-08-17 section 8.1: one state machine
      # stays one writer, so maintenance runs after the merge that owns
      # these tables rather than on a separate schedule. $$.Execution.StartTime
      # is the tick this execution was scheduled for, which is what the
      # minute/hour gates below key off (spec 2026-08-17, assumption M3).
      DeriveMaintenanceClock = {
        Type = "Pass"
        Parameters = {
          "hour.$"   = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 0)"
          "minute.$" = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 1)"
        }
        ResultPath = "$.clock"
        Next       = "IsTopOfHour"
      }

      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverTrades"
        }]
        Default = "MaintenanceDone"
      }

      OptimizeSilverTrades = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["silver_trades"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeQuarantine"
      }

      OptimizeQuarantine = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["silver_trades_quarantine"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeGold"
      }

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

      VacuumSilverTrades = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_trades"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumQuarantine"
      }

      VacuumQuarantine = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_trades_quarantine"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumGold"
      }

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

- [ ] **Step 3: Validate offline**

Run: `make validate-aws`
Expected: `terraform validate` passes, `terraform fmt -check` passes (run `terraform fmt -recursive infra/modules/native_medallion` first if it does not), and `scripts/native_render_parity.sh` passes — this is what proves the ASL's `templatefile()` calls render the same bytes `render.maintenance_statements()` produces in Task 1.

If `terraform fmt` or `validate` fails, fix the reported line before proceeding — do not skip validation for a Terraform-only change.

- [ ] **Step 4: Commit**

```bash
git add infra/modules/native_medallion/main.tf
git commit -m "feat(infra): add OPTIMIZE/VACUUM tail states to fdai-native-microbatch"
```

---

### Task 4: Maintenance tail state in `fdai-native-enrichment-merge-perp`

**Files:**
- Modify: `infra/modules/native_enrichment/main.tf`

**Interfaces:**
- Consumes: same as Task 3, plus `var.merge_lookback_days` (this module's name for the lookback variable — confirmed in `infra/modules/native_enrichment/variables.tf`).
- Produces: locals `local.perp_maintenance_predicate`, `local.optimize_sql` (map, one entry: `silver_perp_context`), `local.vacuum_sql` (map, two entries: `silver_perp_context`, `silver_macro` — `silver_macro`'s entry is consumed by Task 5, not this task); new ASL states in `aws_sfn_state_machine.merge_perp`: `DeriveMaintenanceClock`, `IsTopOfHour`, `OptimizeSilverPerpContext`, `IsTopOfDay`, `VacuumSilverPerpContext`, `MaintenanceDone`.

- [ ] **Step 1: Add the maintenance locals**

In `infra/modules/native_enrichment/main.tf`, inside the existing `locals { ... }` block, after `athena_retry`, add:

```hcl
  # --- maintenance SQL, spec 2026-08-19 section 3 ---------------------------
  #
  # silver_perp_context takes 5-minute commits like the trade tables, so it
  # gets the same hourly-OPTIMIZE/daily-VACUUM treatment. silver_macro takes
  # about one commit a day and gets VACUUM only, added directly in the macro
  # state machine below with no clock gate: at that cadence, "once per
  # execution" already IS "once a day."
  perp_maintenance_predicate = "snapshot_ts >= current_date - interval '${var.merge_lookback_days}' day"

  optimize_sql = {
    silver_perp_context = templatefile("${var.sql_dir}/optimize_table.sql", {
      database            = var.glue_database_name
      table               = "silver_perp_context"
      partition_predicate = local.perp_maintenance_predicate
    })
  }

  vacuum_sql = {
    for table in ["silver_perp_context", "silver_macro"] : table => templatefile("${var.sql_dir}/vacuum_table.sql", {
      database = var.glue_database_name
      table    = table
    })
  }
```

- [ ] **Step 2: Change `MergePerpContext` to hand off to maintenance**

In the same file, inside `aws_sfn_state_machine.merge_perp`'s `definition`, change:

```hcl
      MergePerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_perp_context
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
      MergePerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_perp_context
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "DeriveMaintenanceClock"
      }

      DeriveMaintenanceClock = {
        Type = "Pass"
        Parameters = {
          "hour.$"   = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 0)"
          "minute.$" = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 1)"
        }
        ResultPath = "$.clock"
        Next       = "IsTopOfHour"
      }

      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverPerpContext"
        }]
        Default = "MaintenanceDone"
      }

      OptimizeSilverPerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["silver_perp_context"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "IsTopOfDay"
      }

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

- [ ] **Step 3: Validate offline**

Run: `make validate-aws`
Expected: same three checks as Task 3, Step 3, now covering both modules.

- [ ] **Step 4: Commit**

```bash
git add infra/modules/native_enrichment/main.tf
git commit -m "feat(infra): add OPTIMIZE/VACUUM tail state to fdai-native-enrichment-merge-perp"
```

---

### Task 5: Maintenance tail state in `fdai-native-enrichment-merge-macro`

**Files:**
- Modify: `infra/modules/native_enrichment/main.tf`

**Interfaces:**
- Consumes: `local.vacuum_sql["silver_macro"]`, produced by Task 4's locals addition (both merge state machines are defined in this same file and share `locals { ... }`).
- Produces: one new ASL state in `aws_sfn_state_machine.merge_macro`: `VacuumSilverMacro`.

- [ ] **Step 1: Change `MergeMacro` to hand off to a single VACUUM state**

In `infra/modules/native_enrichment/main.tf`, inside `aws_sfn_state_machine.merge_macro`'s `definition`, change:

```hcl
      MergeMacro = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_macro
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
      MergeMacro = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_macro
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumSilverMacro"
      }

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

- [ ] **Step 2: Validate offline**

Run: `make validate-aws`
Expected: same three checks, all passing.

- [ ] **Step 3: Commit**

```bash
git add infra/modules/native_enrichment/main.tf
git commit -m "feat(infra): add VACUUM tail state to fdai-native-enrichment-merge-macro"
```

---

### Task 6: `verify_maintenance.sql` and its Make target

**Files:**
- Create: `awsnative/sql/verify_maintenance.sql`
- Modify: `Makefile`
- Test: `tests/awsnative/test_render.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (this file is read raw by `awsnative.query`, the same way `verify_silver_gold.sql` is — no `render()` step, no `${database}` placeholder, because `AthenaRunner` already scopes queries to the configured database via `QueryExecutionContext`).
- Produces: `make maintenance-verify-aws`.

- [ ] **Step 1: Write the failing test**

Append to `tests/awsnative/test_render.py`:

```python
def test_verify_maintenance_sql_is_two_statements_over_every_maintained_table() -> None:
    from awsnative.athena import split_statements

    text = (render.SQL_DIR / "verify_maintenance.sql").read_text()
    statements = split_statements(text)
    assert len(statements) == 2
    for table in render.MAINTAINED_TABLES:
        assert f"'{table}'" in statements[0]
        assert f"'{table}'" in statements[1]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/awsnative/test_render.py -k verify_maintenance -v`
Expected: FAIL, `FileNotFoundError`.

- [ ] **Step 3: Create `awsnative/sql/verify_maintenance.sql`**

```sql
-- Maintenance health, per maintained table. Run after a housekeeping pass to
-- confirm OPTIMIZE and VACUUM did what section 8.4 of
-- 2026-08-17-iceberg-table-maintenance-design.md asks for: file count and
-- average file size trending down after OPTIMIZE, snapshot count bounded
-- after VACUUM. Numbers, not a pass/fail: read them next to the numbers from
-- before the pass ran.

-- 1. File count and average size, active snapshot only.
SELECT 'silver_trades' AS table_name, count(*) AS file_count,
       avg(file_size_in_bytes) / 1e6 AS avg_file_size_mb
FROM "silver_trades$files"
UNION ALL
SELECT 'silver_trades_quarantine', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_trades_quarantine$files"
UNION ALL
SELECT 'gold_bars_1m', count(*), avg(file_size_in_bytes) / 1e6
FROM "gold_bars_1m$files"
UNION ALL
SELECT 'silver_perp_context', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_perp_context$files"
UNION ALL
SELECT 'silver_macro', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_macro$files"
ORDER BY table_name;

-- 2. Snapshot count and the oldest snapshot's age. A count that only grows
--    means VACUUM is not running or is not committing.
SELECT 'silver_trades' AS table_name, count(*) AS snapshot_count,
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at)) AS oldest_snapshot_age_seconds
FROM "silver_trades$snapshots"
UNION ALL
SELECT 'silver_trades_quarantine', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_trades_quarantine$snapshots"
UNION ALL
SELECT 'gold_bars_1m', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "gold_bars_1m$snapshots"
UNION ALL
SELECT 'silver_perp_context', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_perp_context$snapshots"
UNION ALL
SELECT 'silver_macro', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_macro$snapshots"
ORDER BY table_name;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/awsnative/test_render.py -v`
Expected: PASS, all tests in the file.

- [ ] **Step 5: Add the Make target**

In `Makefile`, change the `.PHONY` line for this section:

```makefile
.PHONY: ddl-aws microbatch-aws verify-aws sfn-logs-aws
```

to:

```makefile
.PHONY: ddl-aws microbatch-aws verify-aws sfn-logs-aws maintenance-verify-aws
```

Then add, directly after the existing `verify-aws:` target:

```makefile
maintenance-verify-aws:
	uv run --group awsnative python -m awsnative.query \
	  --database "$(TF_DB)" --workgroup "$(TF_WG)" \
	  --file awsnative/sql/verify_maintenance.sql
```

- [ ] **Step 6: Lint, typecheck, commit**

Run: `make lint && make typecheck`

```bash
git add awsnative/sql/verify_maintenance.sql Makefile tests/awsnative/test_render.py
git commit -m "feat(awsnative): add verify_maintenance.sql and make maintenance-verify-aws"
```

---

### Task 7: Deploy, verify against a live account, update docs

**Files:**
- Modify: `docs/DATA_LAYER.md` (§13)
- Modify: `docs/DATA_LAYER_NEXT_STEPS.md` (§3.2)

This task has no offline test — it is the live-account verification the earlier tasks' offline checks cannot substitute for, per this repo's own convention (`make microbatch-aws`'s docstring: "a failure you caused here is easier to read than one that arrived on a timer").

- [ ] **Step 1: Deploy**

```bash
terraform -chdir=infra/envs/native apply
```

Review the plan before confirming — expect only the two modified state machines and no IAM changes (per Global Constraints above; a proposed IAM change here means something in this plan's assumption about existing permissions was wrong, and is a stop-and-investigate signal, not a proceed-and-fix-later one).

- [ ] **Step 2: Verify M1 — `OPTIMIZE ... WHERE` accepts the predicate**

```bash
make microbatch-aws
```

Expected: `SUCCEEDED`. If it fails with an Athena error naming the `WHERE` clause, M1 is false — apply the design's own named fallback (spec 2026-08-17, section 9): drop the `WHERE` clause in `optimize_table.sql` and optimize the whole table, since these tables are small.

- [ ] **Step 3: Verify M3 — the execution-time clock derivation**

```bash
aws stepfunctions get-execution-history \
  --execution-arn "$(aws stepfunctions list-executions \
      --state-machine-arn "$(terraform -chdir=infra/envs/native output -raw microbatch_state_machine_arn)" \
      --max-results 1 --query 'executions[0].executionArn' --output text)" \
  --query "events[?type=='PassStateExited'].stateExitedEventDetails.output" --output text
```

Expected: JSON containing `"clock":{"hour":"HH","minute":"MM"}` matching the wall-clock time the execution actually started at. If `hour`/`minute` are missing or malformed, the nested `States.StringSplit`/`States.ArrayGetItem` calls did not parse as expected — apply the design's named fallback (spec 2026-08-17, section 9, M3): a second `aws_scheduler_schedule` at `rate(1 hour)` for `OPTIMIZE`, with an overlap guard that counts executions of both state machines.

- [ ] **Step 4: Verify the maintenance passes ran**

```bash
make maintenance-verify-aws
```

Expected: rows for all five tables in both result sets, printed with `file_count`, `avg_file_size_mb`, `snapshot_count`, `oldest_snapshot_age_seconds`. Save this output — it is the "before" baseline; re-run after a few hours of the merge/enrichment schedules running to confirm file counts stop growing unbounded and snapshot age stays under the 3600-second retention.

- [ ] **Step 5: Update `docs/DATA_LAYER.md` §13**

Remove this bullet (it is no longer true):

```markdown
- **No table maintenance.** Nothing runs `OPTIMIZE` or `VACUUM`. The
  insert-only tables accumulate small files and `gold_bars_1m` accumulates
  merge-on-read delete files on every micro-batch. The symptom shows up first
  as rising query time.
```

- [ ] **Step 6: Update `docs/DATA_LAYER_NEXT_STEPS.md` §3.2**

Change the section heading from `### 3.2 Maintenance, from proposal to running` to `### 3.2 Maintenance — running` and replace the paragraph body with a short status note pointing at this plan and the two specs, e.g.:

```markdown
Implemented: [`2026-08-19-iceberg-maintenance-extension.md`](../superpowers/plans/2026-08-19-iceberg-maintenance-extension.md)
extends [`2026-08-17-iceberg-table-maintenance-design.md`](../superpowers/specs/2026-08-17-iceberg-table-maintenance-design.md)
from 3 to 6 tables (adding `silver_perp_context` and `silver_macro`) and runs
`OPTIMIZE`/`VACUUM` as tail states of the state machines that already write
each table. `make maintenance-verify-aws` reports current file, delete-file,
and snapshot counts.
```

- [ ] **Step 7: Commit the doc updates**

```bash
git add docs/DATA_LAYER.md docs/DATA_LAYER_NEXT_STEPS.md
git commit -m "docs: record Iceberg maintenance as implemented, not proposed"
```

---

## Self-Review Notes

- **Spec coverage**: section 3.1 (parameterized template) → Task 1; section 3.1's per-table table → Tasks 3–5; section 3.2 (`silver_macro` VACUUM-only) → Task 1 test + Task 5; section 3.3 (`backfill_manifest` deferred) → intentionally no task, matching the spec's own instruction not to design for a writer that does not exist.
- **Assumptions verified live, not assumed**: M1 and M3 from the 2026-08-17 spec are explicitly checked in Task 7 rather than treated as given, each with its named fallback restated.
- **No IAM task**: confirmed by reading the current policies in both modules before writing this plan (see Global Constraints) rather than assumed from the spec text alone.
