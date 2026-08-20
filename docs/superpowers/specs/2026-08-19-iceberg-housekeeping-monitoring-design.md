# Iceberg Housekeeping, Monitoring, and Dashboards — Design

**Date:** 2026-08-19
**Status:** proposal, no implementation
**Scope:** the AWS-native workstream only. Housekeeping and monitoring for the
six Iceberg tables in `fdai_native`, plus one new metrics table. Not the
Databricks/Delta workstream, not cross-workstream reconciliation, not stage
N4's backfill tables.

This document extends
[`2026-08-17-iceberg-table-maintenance-design.md`](2026-08-17-iceberg-table-maintenance-design.md)
rather than replacing it. That design is correct and unimplemented; read it
first. It covers 3 of the 6 Iceberg tables that exist today because it
predates `silver_perp_context` and `silver_macro`. Sections 1 to 3 below
close that gap. Sections 4 to 6 are new: a monitoring pipeline and a
dashboard, neither of which existed in any form before this document.

---

## 1. Why this is one document, not two

Housekeeping and monitoring share a mechanism, not just a topic. Both need to
run on a schedule tied to each table's write cadence, both read the same
Iceberg metadata (`$files`, `$partitions`, `$snapshots`), and both must not
become a second writer on a table that already has one. The 2026-08-17
design's central finding — add a tail state to the state machine that
already owns the table, so one state machine stays one writer — applies to
monitoring exactly as it applies to `OPTIMIZE` and `VACUUM`. This document
reuses that finding rather than inventing a second scheduling mechanism.

## 2. Current state

| Table | Partition | Commits from | Cadence | Maintained today | Monitored today |
|---|---|---|---|---|---|
| `silver_trades` | `(instrument_id, day(event_ts))` | `fdai-native-microbatch` | 5 min | no | freshness/counts, read on demand via `make dashboard-aws` |
| `silver_trades_quarantine` | `(ingest_date)` | `fdai-native-microbatch` | 5 min | no | quarantine rate, read on demand |
| `gold_bars_1m` | `(instrument_id, day(window_end_ts))` | `fdai-native-microbatch` | 5 min | no | freshness, read on demand |
| `silver_perp_context` | `(instrument_id, day(snapshot_ts))` | `fdai-native-enrichment-merge-perp` | 5 min | no | freshness, read on demand |
| `silver_macro` | `(series_id)` | `fdai-native-enrichment-merge-macro` | ~1/day | no | vintages, read on demand |
| `backfill_manifest` | `(tier)` | stage N4, unbuilt | n/a | n/a | n/a |

"Read on demand" means `awsnative/sql/dashboard/*.sql`, run through
`make dashboard-aws` into a static `dashboard.html`. Nothing runs
unattended, nothing alerts, and nothing keeps history — each run only shows
the tables as they are at that moment.

## 3. Housekeeping: extending the maintenance design to six tables

### 3.1 The tail-state template becomes parameterized

The 2026-08-17 design hardcodes three `OPTIMIZE` statements and three
`VACUUM` statements as named states in `fdai-native-microbatch`. Adding
`silver_perp_context` and `silver_macro` the same way would hardcode three
more into a different state machine. Instead, the maintenance states are
generated from a table list, one entry per Iceberg table:

```text
maintained_tables = {
  silver_trades             = { partition_col = "event_ts",      optimize = "hourly", vacuum = "daily" }
  silver_trades_quarantine  = { partition_col = "ingest_date",   optimize = "hourly", vacuum = "daily" }
  gold_bars_1m              = { partition_col = "window_end_ts", optimize = "hourly", vacuum = "daily" }
  silver_perp_context       = { partition_col = "snapshot_ts",   optimize = "hourly", vacuum = "daily" }
  silver_macro              = { partition_col = null,            optimize = "none",   vacuum = "daily" }
  native_health_metrics     = { partition_col = "metric_ts",     optimize = "hourly", vacuum = "daily" }  # see §5.3
}
```

Each state machine renders only the entries for the tables it writes:
`fdai-native-microbatch` gets three, `fdai-native-enrichment-merge-perp`
gets one (plus its share of `native_health_metrics`, §5.3), and
`fdai-native-enrichment-merge-macro` gets one. The `Choice`-gated tail-state
shape, the SQL template, and the table properties are unchanged from the
2026-08-17 design (§8.1 to §8.3) — this section only changes how many times
that shape gets stamped out and into which state machines.

### 3.2 Why `silver_macro` gets `VACUUM` only

`silver_macro` takes about one commit a day (§8 in `DATA_LAYER.md`). The
2026-08-17 design's own numbers (§7.2) show the small-file problem appears
at 96 commits in an 8-hour session; `silver_macro` would take roughly three
months to reach the same count. Running `OPTIMIZE` against it finds nothing
to rewrite, every time, forever — a real cost of zero per the design's own
cost model, but also a state that never does anything, which is a bad
sign to leave in a pipeline unexamined. `VACUUM` still runs daily, because
snapshot and metadata growth is a function of commit count regardless of
file size, and a five-vintage `CPIAUCSL` revision history is exactly the
kind of snapshot history worth trimming on the same schedule as everything
else.

### 3.3 `backfill_manifest` stays out

Stage N4 is unbuilt. Designing maintenance for a writer that does not exist
would be a guess dressed as a spec. When N4 ships, add one entry to the
table list in §3.1 and it inherits the same mechanism — no new design
needed then either.

## 4. Monitoring pipeline

### 4.1 A second tail state, same state machines

`CollectHealthMetrics` is a new tail state added after the maintenance
states in each of the three writer state machines. It runs once per
execution of its state machine — every 5 minutes in
`fdai-native-microbatch` and `fdai-native-enrichment-merge-perp`, once a day
in `fdai-native-enrichment-merge-macro` — and computes, per table it is
responsible for:

- freshness lag (reusing `awsnative/sql/dashboard/02_freshness.sql`'s query
  shape)
- quarantine rate, for `silver_trades_quarantine` only (reusing
  `03_quarantine.sql`)
- row count (reusing `01_layer_counts.sql`)
- file count, average file size, and small-file percentage, from
  `"table"$partitions` and `"table"$files`
- delete-file count, from `"table"$files` (assumption M2 in the 2026-08-17
  design covers whether this column exists; if it does not, this KPI is
  dropped and file count alone stands in, per that design's fallback)
- snapshot count and oldest snapshot age, from `"table"$snapshots`

No new state machine, no new schedule, no new IAM role for compute — the
existing `athena:StartQueryExecution` permission already covers these
read-only queries against the same tables the state machine already reads
or writes.

### 4.2 Two destinations per tick

Each `CollectHealthMetrics` execution writes its result to two places:

1. **CloudWatch**, via `PutMetricData`, namespace `FDAI/Native`, dimensioned
   by `TableName`. This is what the alarms in §4.4 read.
2. **`native_health_metrics`** (§4.3), via the same parameterized
   `MERGE INTO` template used for the other tables, so QuickSight has a
   history to chart rather than only a live snapshot.

Both destinations receive the same numbers computed once. Writing to two
places is not two computations.

### 4.3 The new table: `native_health_metrics`

Iceberg, partitioned by `day(metric_ts)`, one row per (table monitored,
tick). Insert-only — a health reading is never corrected in place, only
superseded by the next tick — so it needs the same `WHEN NOT MATCHED THEN
INSERT`-only shape as `silver_trades`, and the same reasoning: re-running a
tick must not change a stored reading.

| Column | Type | Note |
|---|---|---|
| `metric_ts` | timestamp | when the tick ran |
| `metric_date` | date | partition column, `day(metric_ts)` |
| `table_name` | string | which of the six tables this row describes |
| `tier` | string | `fast` or `slow`, from §4.1's cadence |
| `row_count` | bigint | |
| `file_count` | bigint | |
| `avg_file_size_mb` | double | |
| `small_file_pct` | double | percent of active files under 100 MB |
| `delete_file_count` | bigint | null if M2 (§4.1) is false |
| `snapshot_count` | bigint | |
| `oldest_snapshot_age_seconds` | bigint | |
| `freshness_lag_seconds` | bigint | null for tables §4.1 does not compute it for |
| `quarantine_rate_pct` | double | null except for `silver_trades_quarantine` |

This table is itself insert-only Iceberg, so it goes into the `maintained_tables`
list in §3.1 (`optimize = hourly, vacuum = daily`) rather than being
exempted. Monitoring the monitor is the same problem as monitoring anything
else in this lake; it does not get a separate answer.

### 4.4 CloudWatch Alarms

Every alarm's action is the same SNS topic, `fdai-native-alerts`, subscribed
to one email address at apply time. One topic keeps the alarm list additive
— a new alarm is a new `aws_cloudwatch_metric_alarm` resource, not a new
notification path.

| Alarm | Metric | Threshold | Evaluation | Why this shape |
|---|---|---|---|---|
| Trades/Gold/Perp freshness | `FDAI/Native FreshnessLagSeconds`, per table | table's own SLO from `DATA_LAYER.md` §7 (e.g. 360s for Silver trades) | 2 of 2 periods, 5 min each | one missed tick at 5-minute cadence is normal; two in a row is not |
| Macro freshness | same metric, `silver_macro` | 30 hours (24h SLO plus a buffer) | 1 of 1 period, 1 day | a daily cadence must not alarm on its own normal gap |
| Quarantine rate | `FDAI/Native QuarantineRatePct` | 0.1% (the design's own SLO ceiling, `DATA_LAYER.md` §8) | 2 of 2 periods, 5 min each | matches the trades tier's cadence |
| Maintenance stalled | `FDAI/Native SmallFilePct`, per fast-tier table | still above `optimize_rewrite_data_file_threshold`'s implied level 2 hours after the hourly `OPTIMIZE` should have run | 1 of 1 period, 2 hours | implements Practice 9 from the 2026-08-17 design: alarm on maintenance, not only on the pipeline |
| Writer or maintenance execution failed | `AWS/States ExecutionsFailed`, per state machine | ≥ 1 | 1 of 1 period, 5 min or 1 day to match the state machine's own cadence | free AWS-provided metric, no custom code, catches a state machine that stopped entirely |

### 4.5 IAM: additive only

The three merge-role policies (`native_medallion`'s microbatch role, and
`native_enrichment`'s `merge_sfn` role for both merges) each gain:

- `cloudwatch:PutMetricData`, unscoped by resource (the action does not
  support resource-level scoping)
- `glue:GetTable`, `glue:UpdateTable`, and the S3 read/write/delete
  statements already granted for the other tables, extended to
  `native_health_metrics`'s prefix

No new role, no new trust policy. This mirrors the 2026-08-17 design's own
finding that the tail-state shape needs nothing new on the IAM side beyond
scope extensions to a role that already exists.

## 5. Dashboard: QuickSight

### 5.1 Two datasets, two refresh schedules

`native_health_metrics` feeds two QuickSight datasets, not one, because the
two tiers have different freshness needs and mixing them means the slower
one gates the faster one:

- **Fast dataset**: `silver_trades`, `silver_trades_quarantine`,
  `gold_bars_1m`, `silver_perp_context` rows. SPICE, scheduled refresh at
  the shortest interval QuickSight's scheduled refresh supports for this
  account (hourly refresh has been generally available across editions;
  whether a 15-minute interval is available here is unverified, §6, Q3).
- **Slow dataset**: `silver_macro` rows. SPICE, scheduled refresh once
  daily, timed to run after `fdai-native-enrichment-merge-macro`'s own
  daily tick has written that day's row.

Each dataset backs its own analysis and dashboard: a fast-tier health
dashboard and a slow-tier health dashboard. Splitting them means a failed
slow refresh never blocks the fast dashboard, and the fast dashboard's more
frequent refresh never re-scans the macro rows that cannot have changed.

### 5.2 Terraform-managed, so the weekly wipe is a non-event

`aws_quicksight_data_source` (pointing at the `fdai-native` Athena
workgroup), `aws_quicksight_data_set` (one per tier), `aws_quicksight_analysis`,
and `aws_quicksight_dashboard` all go in a new `native_dashboards` module.
`make up-aws` recreates them the same way it recreates the Iceberg tables
today — there is nothing QuickSight-specific about surviving the wipe once
the resources are code.

### 5.3 Relationship to the existing local dashboard

`awsnative/dashboard/` and `make dashboard-aws` are unchanged. QuickSight
answers "is everything healthy, right now, at a glance" from stored history;
the local HTML dashboard answers "what does the enrichment show" with the
richer, one-off views (funding, positioning, macro vintages) that a
KPI-oriented QuickSight dashboard is the wrong shape for. Neither replaces
the other.

## 6. Assumptions to verify

Each has a named fallback, matching the practice the 2026-08-17 design set.

| # | Assumption | Verify with | Fallback if false |
|---|---|---|---|
| Q1 | QuickSight is enabled (subscribed) in the sandbox account | check the QuickSight console, or `aws quicksight describe-account-subscription` | enable it once by hand before the first `terraform apply`; this step cannot be scripted from outside the account |
| Q2 | QuickSight's Athena/Glue/S3 access can be granted without a new IAM role, through QuickSight's own managed service role | `aws quicksight describe-account-settings`, or attempt the data source creation | attach the needed policies to `aws-quicksight-service-role-v0` directly, still no new role type |
| Q3 | SPICE scheduled refresh supports a 15-minute interval on this account's edition | attempt to schedule one | fall back to hourly refresh for the fast dataset; freshness lag on the dashboard becomes "up to 1 hour stale" rather than "up to 15 minutes stale" |
| M2 | `$files` exposes a column distinguishing delete files from data files (carried over from the 2026-08-17 design, now also needed for `native_health_metrics.delete_file_count`) | `DESCRIBE "db"."gold_bars_1m$files"` | drop `delete_file_count`; infer delete pressure from `small_file_pct` and query latency alone |
| A1 | A Step Functions `Choice` state's time-based gating extends cleanly to a second tail state (`CollectHealthMetrics`) after the maintenance states, inside the 5-minute window | one test execution, timed | move `CollectHealthMetrics` to run every other tick (10-minute effective cadence) if the window is too tight |

## 7. Rejected alternatives

- **A CloudWatch Dashboard instead of QuickSight.** Cheaper (no per-author
  cost) and simpler to keep in Terraform, but you chose QuickSight for the
  richer chart types; recorded here because it is the recommended
  alternative if the QuickSight cost (§8) or Q1/Q2 above prove to be a
  blocker.
- **One QuickSight dataset covering all six tables.** Simpler to define,
  but couples the slow tier's daily refresh to the fast tier's hourly one,
  which means either the fast dashboard is only as fresh as the slowest
  table or every refresh re-scans rows that provably have not changed.
- **A separate schedule and state machine for monitoring.** Matches the
  2026-08-17 design's own rejection of a separate maintenance schedule, for
  the same reason: it introduces a second writer to reason about, where the
  tail-state shape has none.
- **Writing metrics only to CloudWatch, skipping `native_health_metrics`.**
  CloudWatch custom metrics expire after 15 months but are not efficient to
  chart in QuickSight, and QuickSight cannot use CloudWatch as a native
  Athena-style data source without another hop. A dedicated table is one
  extra Iceberg table with a well-understood shape, not a new pattern.

## 8. What this does not cover

- The Databricks/Delta workstream. Unity Catalog's system tables and
  Lakeflow pipeline event logs are the native fit there; scoped out per
  your answer in this design's brainstorming.
- Cross-workstream reconciliation (`DATA_LAYER_NEXT_STEPS.md` §3.3).
  Related, separately ranked, and out of scope here.
- `backfill_manifest` housekeeping and monitoring, blocked on stage N4.
- The point-in-time boundary (stage N5). `NEXT_STEPS.md` ranks it above
  maintenance for a reason unrelated to this document: it is about what
  can be read, not about what needs upkeep. This document does not change
  that ordering.

## 9. Cost

Additive to the 2026-08-17 design's `~$4` Athena maintenance line, itself
inside the stack's existing `~$25 to $35/mo`:

- `CollectHealthMetrics`: single-digit-megabyte Athena scans, same order as
  the maintenance queries it sits beside. Negligible.
- CloudWatch: custom metrics and alarms are priced per metric and per
  alarm, low single-digit dollars a month at this table count.
- QuickSight: the new, material line. One author, Standard or Enterprise
  edition, is roughly $9 to $24/month, independent of how much data it
  reads — this is the number to weigh against the "richer charts" reason
  for choosing it over a CloudWatch Dashboard in §7.

## Sources

- [`2026-08-17-iceberg-table-maintenance-design.md`](2026-08-17-iceberg-table-maintenance-design.md)
- [`DATA_LAYER.md`](../DATA_LAYER.md) §4, §6, §7, §8, §13
- [`DATA_LAYER_NEXT_STEPS.md`](../DATA_LAYER_NEXT_STEPS.md) §3.2, §3.4
- [QuickSight scheduled SPICE refresh](https://docs.aws.amazon.com/quicksight/latest/user/refreshing-imported-data.html)
- [QuickSight pricing](https://aws.amazon.com/quicksight/pricing/)
- [CloudWatch custom metrics pricing](https://aws.amazon.com/cloudwatch/pricing/)
