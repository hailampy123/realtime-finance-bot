# Iceberg Table Maintenance on AWS — Design

**Date:** 2026-08-17
**Status:** proposal, no implementation
**Scope:** table maintenance and its operation. Not catalog choice, not schema
evolution, not access control, not disaster recovery.

Sections 2 to 6 are general. They apply to any Iceberg table on AWS. Section 7
onward applies the general result to the three Iceberg tables in this repo.

---

## 1. Purpose

Apache Iceberg gives you atomic commits, row-level updates and time travel on
plain object storage. It pays for those properties with files. A table that
nobody maintains gets slower and more expensive on a curve set by its commit
rate, not by its age.

This document answers three questions:

1. Which maintenance jobs exist, and what does each one prevent?
2. Which AWS service should run each job, and at what cost?
3. What do the three tables in this repo need, and when?

## 2. Why the format needs maintenance

Iceberg never edits a file in place. A commit writes new files and publishes a
new snapshot that points at them. Five layers sit between the catalog and the
data:

```text
Glue table (a pointer)  ->  metadata JSON  ->  manifest list  ->  manifest files
                                                                     |
                                                        data files + delete files
```

Every commit adds one metadata JSON file, one manifest list and at least one
manifest. Two costs follow, and they behave differently.

**Read cost** grows with the number of data files, the number of delete files
per data file, and the number of manifests the planner must open. Each small
file costs one S3 `GET` and one Parquet footer read. Each delete file must be
applied to the data file it references before the engine returns a row.

**Storage cost** grows with the files that no live snapshot references. Those
files are unreachable but not deleted, because a commit does not delete.

The important property: **both costs track the number of commits.** A table
that takes one commit a day stays healthy for years. A table that takes 288
commits a day needs attention on the first day. Table age is not the variable.

## 3. The five maintenance jobs

| # | Job | Iceberg action | What it fixes | Symptom if you skip it |
|---|---|---|---|---|
| 1 | Data compaction | `rewrite_data_files`, bin-pack | many small data files | query latency and scan cost rise; planning time grows |
| 2 | Delete-file compaction | `rewrite_data_files` with a delete threshold | merge-on-read delete files pile up | every read applies deletes; latency grows with the update count |
| 3 | Snapshot expiry | `expire_snapshots` | snapshot and manifest history | metadata grows per commit; superseded data files never free |
| 4 | Orphan file removal | `remove_orphan_files` | files from failed or retried jobs | silent storage cost; no correctness effect |
| 5 | Manifest rewrite | `rewrite_manifests` | manifests that no longer match the partition layout | the planner opens manifests it does not need |

Two more operations travel with maintenance but optimize reads rather than
repair the table:

- **Sort and Z-order.** A compaction pass can also cluster rows by the columns
  your queries filter on. Bin-pack only changes file size.
- **Column statistics.** Iceberg Puffin statistics let a cost-based optimizer
  choose join order and filters. Athena reads them when the table property
  `use_iceberg_statistics` allows it.

Jobs 1 and 2 change what a query reads. Jobs 3 and 4 change what you pay to
store. Job 5 changes what the planner reads. Treat them as separate decisions
with separate cadences.

## 4. The four AWS execution surfaces

### 4.1 Amazon Athena: `OPTIMIZE` and `VACUUM`

Two SQL statements cover four of the five jobs.

```sql
OPTIMIZE [db.]table REWRITE DATA USING BIN_PACK [WHERE predicate]
VACUUM   [db.]table
```

`OPTIMIZE` does jobs 1 and 2. `VACUUM` does jobs 3 and 4. Athena has no
statement for job 5.

Athena accepts only a predefined list of Iceberg table properties. The list
that controls maintenance:

| Property | Default | Effect |
|---|---|---|
| `optimize_rewrite_data_file_threshold` | `5` | below this count of files that need a rewrite, `OPTIMIZE` skips the partition |
| `optimize_rewrite_delete_file_threshold` | `2` | a data file is rewritten once this many delete files reference it |
| `vacuum_max_snapshot_age_seconds` | `432000` (5 days) | maximum snapshot age to retain; also the orphan-file age cut-off |
| `vacuum_min_snapshots_to_keep` | `1` | minimum snapshots to retain; overrides the age property |
| `vacuum_max_metadata_files_to_keep` | `100` | previous metadata files to retain |

Four limits worth knowing before you rely on this surface:

- The `WHERE` clause accepts partition columns only. A non-partition column
  fails the query.
- `VACUUM` needs the Iceberg data in a folder, not at a bucket root. At a bucket
  root it fails with `GENERIC_INTERNAL_ERROR: Path missing in file system
  location`.
- **Grant `s3:DeleteObject` to the query execution role before you run
  `VACUUM`.** Without it the query succeeds and deletes nothing. The failure is
  silent.
- The documented property list has no target-file-size entry, so `OPTIMIZE`
  uses the Iceberg default target size.

Cost: Athena charges `OPTIMIZE` by the data it scans during the rewrite. A pass
with nothing to rewrite scans nothing and costs nothing. `VACUUM` makes S3 API
calls, so S3 request charges apply.

Athena engine version 3 creates and writes Iceberg v2 tables only. It has no
support for the v3 spec, so deletion vectors are not available.

### 4.2 AWS Glue Data Catalog table optimizers

The Glue Data Catalog runs three managed optimizers per table: `compaction`,
`retention` and `orphan_file_deletion`. You configure them once and Glue
schedules them.

- The compaction optimizer supports bin-pack, sort and Z-order. Since December
  2024 it also compacts merge-on-read position and equality delete files, and it
  commits partial progress to reduce conflicts.
- Compaction starts when a table or one of its partitions holds **more than 100
  files**, each smaller than 75% of the target size. The target comes from
  `write.target-file-size-bytes` and defaults to 512 MB.
- Snapshot retention and orphan file deletion delete at most 1,000,000 files per
  run. Files above that limit stay as orphans.

Cost: **$0.44 per DPU-hour**, billed with a one-minute minimum.

Requirements and traps:

- The optimizer assumes an IAM role that you pass. The role needs S3 read,
  write and delete, `glue:GetTable`, `glue:UpdateTable`, CloudWatch Logs, and a
  trust policy for `glue.amazonaws.com`. The caller needs `iam:PassRole`.
- **Give every table with an optimizer its own S3 location.** If two tables
  share a location, one table's orphan removal deletes the other table's live
  files.
- After four consecutive failures Glue **suspends the optimizer**. Maintenance
  then stops without an error at the table. Alarm on it.
- Terraform has `aws_glue_catalog_table_optimizer`. An open provider issue asks
  for compaction parameters, so check your provider version before you plan to
  set a strategy or a target size in code.

### 4.3 Spark on AWS Glue or Amazon EMR

Iceberg's stored procedures run on any Spark that has the Iceberg runtime. This
surface is the only one that covers all five jobs and the only one that exposes
the full parameter set.

Use it when you need one of these:

- `rewrite_manifests`, which no managed surface offers.
- A sort or Z-order pass over a large table.
- `PARTIAL_PROGRESS_ENABLED`, so one collision costs one commit instead of the
  whole rewrite.
- A custom `where` predicate that excludes partitions an active writer holds.
- Control over `MAX_FILE_GROUP_SIZE_BYTES` (100 GB default) and
  `MAX_CONCURRENT_FILE_GROUP_REWRITES` for cluster sizing.

The Iceberg bin-pack defaults are worth knowing, because Athena and Glue both
build on them: `MIN_FILE_SIZE_BYTES` is 75% of the target size,
`MAX_FILE_SIZE_BYTES` is 180%, `MIN_INPUT_FILES` is 5, and
`DELETE_FILE_THRESHOLD` is unset, which means Spark does **not** merge delete
files unless you ask. Athena differs here: its delete threshold defaults to 2.

Cost: DPU-hours or EMR instance hours, with a floor per job. Wrong for small
frequent passes, right for large occasional ones.

### 4.4 Amazon S3 Tables

S3 Tables is a storage-managed Iceberg catalog. Maintenance is on by default:

- **File compaction**, per table. Target file size defaults to 512 MB, and you
  can set it between 64 MB and 512 MB.
- **Snapshot management**, per table. `MinimumSnapshots` defaults to 1 and
  `MaximumSnapshotAge` to 120 hours.
- **Unreferenced file removal**, per table bucket.
- **Record expiration**, optional.

Cost has a different shape from the other three surfaces: you pay per 1,000
objects processed and per GB processed, plus S3 Tables storage rates. At a high
update rate that bill can exceed the ingest bill, so measure it rather than
assume it.

S3 Tables suits a new table where you want storage to manage itself. It does not
suit an existing table in a plain S3 bucket, because you would migrate the data.

### 4.5 The surfaces compared

| | Athena | Glue optimizers | Spark (Glue/EMR) | S3 Tables |
|---|---|---|---|---|
| Jobs 1, 2 | yes | yes | yes | yes |
| Job 3 | yes | yes | yes | yes |
| Job 4 | yes | yes | yes | yes |
| Job 5 | no | no | yes | no |
| Sort / Z-order | no | yes | yes | no |
| Per-partition predicate | yes | no | yes | no |
| Who schedules it | you | Glue | you | S3 |
| New IAM role | no | yes | yes | yes |
| Cost when idle | zero | zero | zero | zero |
| Cost basis | bytes scanned | $0.44/DPU-h | DPU or instance hours | per 1,000 objects + per GB |
| Code to own | SQL | none | a Spark job | none |

## 5. Ten practices from production platforms

Each practice names the failure it prevents. That is the reason to keep it.

1. **Compact cold partitions only.** Put a predicate on the maintenance pass so
   it never touches a partition an active writer holds: `where => 'dt <
   current_date'`. A data conflict between a writer and a rewrite raises a
   non-retryable `ValidationException`. A metadata conflict is one Iceberg
   resolves by itself. The predicate converts the first kind into the second.
2. **Enable partial progress on large rewrites.** Without it, one collision
   discards the whole pass and you pay to redo all of it.
3. **Make the retry settings asymmetric.** AWS recommends 10 commit retries for
   a streaming writer and 4 for a maintenance job. The writer holds the
   deadline; maintenance can wait.
4. **Run two cadences, not one.** Compaction follows write volume, so hourly or
   daily. Expiry and orphan removal follow your time-travel commitment, so daily
   or weekly. One combined schedule serves one of them badly.
5. **Set the orphan retention longer than your longest job.** Orphan removal
   deletes any unreferenced file older than the cut-off. A job that has written
   files but not yet committed owns unreferenced files. Too short a cut-off
   deletes them mid-flight.
6. **Measure from Iceberg metadata, not from S3 listings.** Athena exposes
   `$files`, `$manifests`, `$history`, `$snapshots` and `$partitions`. File
   count and average file size per partition are the two numbers that decide
   whether compaction is due:

   ```sql
   SELECT partition, file_count, record_count
   FROM "db"."table$partitions" ORDER BY file_count DESC
   ```

7. **Never point an S3 lifecycle expiry rule at an Iceberg table prefix.** A
   lifecycle rule deletes manifest and data files that live snapshots still
   reference, and the table becomes unreadable. Restrict lifecycle rules to
   prefixes no table owns.
8. **Give one table one S3 location.** Shared locations plus orphan removal
   equals unrecoverable data loss.
9. **Alarm on the maintenance job, not only on the pipeline.** Glue suspends an
   optimizer after four failures. A silent stop is the common way maintenance
   dies.
10. **Budget maintenance as its own line.** Compaction reads and rewrites the
    data it fixes. At a high update rate, maintenance can cost more than
    ingestion. Publish the number next to the ingest number.

## 6. Decision guide

| Situation | Surface |
|---|---|
| Athena is already the writer; tables are small to medium | Athena `OPTIMIZE` and `VACUUM` |
| Many tables, no appetite to own a schedule | Glue Data Catalog optimizers |
| You need sort or Z-order clustering | Glue optimizers, or Spark for full control |
| Large tables, or a large backlog of small files | Spark on Glue or EMR, with dynamic scale |
| Manifests no longer match the partition layout | Spark, `rewrite_manifests` |
| A writer holds the partition you must compact | Spark, with a cold predicate and partial progress |
| A new table, and storage should manage itself | S3 Tables |
| Cost when idle must be exactly zero | Athena |

---

## 7. This repo, measured against the general result

### 7.1 Current state

Three Iceberg tables in the Glue Data Catalog, on a plain S3 bucket:

| Table | Partition layout | Merge branches | Location |
|---|---|---|---|
| `silver_trades` | `(instrument_id, day(event_ts))` | `WHEN NOT MATCHED` only | `s3://fdai-native-lake-<acct>/silver_trades/` |
| `silver_trades_quarantine` | `(ingest_date)` | `WHEN NOT MATCHED` only | `.../silver_trades_quarantine/` |
| `gold_bars_1m` | `(instrument_id, day(window_end_ts))` | `WHEN MATCHED` **and** `WHEN NOT MATCHED` | `.../gold_bars_1m/` |

One writer: the `fdai-native-microbatch` state machine, which runs three Athena
`MERGE` statements per execution. Schedule: `rate(5 minutes)`. Lookback:
`microbatch_lookback_days = 1`. Universe: 8 instruments in `config/universe.yaml`.

**Maintenance today: none.** No `OPTIMIZE`, no `VACUUM`, no Glue optimizer, no
table property set beyond `table_type`, `format` and `write_compression`.

Three things are already correct and worth keeping:

- The S3 lifecycle rules target `_athena-results/` and `_errors/` only, so
  practice 7 holds today.
- Each table has its own prefix, so practice 8 holds today.
- The state machine's role already grants `s3:DeleteObject`, so `VACUUM` would
  not fail silently.

### 7.2 What each write pattern produces

The numbers below come from the schedule and the SQL, not from a live account.
Section 9 gives the queries that confirm them.

At `rate(5 minutes)` the pipeline takes 12 commits per hour. The design's stated
operating discipline is about 130 hours a month, so a single 8-hour session
takes about **96 commits per table**.

**`silver_trades`.** The merge has no `WHEN MATCHED` branch, so it only inserts.
Each commit writes at least one data file per touched partition and rewrites
none. With 8 instruments and a 1-day lookback, up to 16 partitions are live.
After one 8-hour session each instrument-day partition holds roughly **96 small
data files**. That crosses Glue's own 100-file trigger threshold inside one
working day.

**`silver_trades_quarantine`.** Same shape, one partition key, lower volume.
Same accumulation, smaller absolute numbers.

**`gold_bars_1m`.** This is the table that matters. The merge has a `WHEN
MATCHED THEN UPDATE` branch, and every minute inside the lookback window is a
candidate for update on every run. Athena writes Iceberg in merge-on-read mode
only, and the mode cannot be changed: "The `UPDATE`, `MERGE INTO`, and `DELETE
FROM` operations always use the merge-on-read approach with positional deletes,
regardless of specified table properties." So each commit adds position delete
files to the 8 live partitions. After one session a partition holds roughly
**96 rounds of delete files**, against a default
`optimize_rewrite_delete_file_threshold` of 2. Every Gold read applies all of
them.

**Snapshots and metadata.** 3 tables at 96 commits each is about 288 snapshots
per session. `vacuum_max_snapshot_age_seconds` defaults to 5 days and
`vacuum_max_metadata_files_to_keep` to 100, but neither default takes effect,
because nothing runs `VACUUM`.

**Orphan files.** The state machine retries every Athena statement 3 times with
backoff. A `MERGE` that writes data files and then loses the optimistic lock
leaves those files behind. Nothing removes them.

### 7.3 What breaks, and when

| Effect | Table | Arrives |
|---|---|---|
| Gold query latency grows with delete-file count | `gold_bars_1m` | within one session |
| Silver scan cost grows with file count | `silver_trades` | within one session |
| Metadata size grows one JSON file per commit | all three | within one session |
| Orphan storage from retried merges | all three | on the first retry |
| Storage cost from unreferenced files | all three | never matters, the account is wiped |

The practical risk is not the bill. It is that a demo query on `gold_bars_1m`
gets slower through a session, and the cause reads like a bug in the SQL.

### 7.4 The original decision, revisited

The DDL comment and design §13 both reject managed Iceberg maintenance:

> Iceberg on plain S3, not S3 Tables: S3 Tables sells managed compaction and
> snapshot expiry, and the weekly wipe means no table lives long enough to need
> either.

That reasoning is correct about **storage** and incorrect about **reads**.
Unreferenced files never accumulate long enough to cost real money, so the
storage half of the argument stands. Read cost is a function of commit count,
and the pipeline advances the commit count 288 times a day while the wipe resets
the clock once a week. The two halves need separate answers.

The rejection of S3 Tables also stands, for the reason given plus one more: it
would mean a migration of live tables.

## 8. Recommendation

**Run Athena `OPTIMIZE` and `VACUUM` from inside the existing state machine.**

### 8.1 Shape

Add maintenance as a tail state of `fdai-native-microbatch`, after `MergeGold`,
behind a `Choice` state that matches the execution start time. With ticks at
`:00`, `:05`, `:10` and so on, a match on the `:00` minute gives one maintenance
pass per hour.

That shape has one property no separate schedule can match: **one state machine
is one writer.** Maintenance cannot collide with a merge, because the merge
already finished in the same execution, and the existing overlap guard stops a
second execution from starting. Practice 1's conflict problem does not arise, so
no cold-partition predicate is needed to avoid it. No new schedule, no new IAM
role, no new state machine.

Practice 3 also drops out. One writer means no commit contention, and Athena's
closed property list would not accept the `commit.retry.*` properties in any
case.

### 8.2 What each pass runs

| Statement | Cadence | Gate on the execution start time |
|---|---|---|
| `OPTIMIZE silver_trades REWRITE DATA USING BIN_PACK WHERE event_ts >= <window>` | hourly | minute `:00` |
| `OPTIMIZE silver_trades_quarantine REWRITE DATA USING BIN_PACK WHERE ingest_date >= <window>` | hourly | minute `:00` |
| `OPTIMIZE gold_bars_1m REWRITE DATA USING BIN_PACK WHERE window_end_ts >= <window>` | hourly | minute `:00` |
| `VACUUM` on each of the three | daily | hour `00`, minute `:00` |

`<window>` is the same `lookback_days` bound the three merge templates already
render, so one Terraform variable controls the merge scope and the maintenance
scope together.

`OPTIMIZE` runs hourly because delete files are the fast-growing problem.
`VACUUM` runs daily because snapshot expiry and orphan removal address storage,
which is the slow problem. Two `Choice` states express the two cadences: one
matches the minute, and a second nested one matches the hour.

The `WHERE` clause here is a cost control, not a conflict guard. Section 8.1
removed the conflict. The predicate keeps `OPTIMIZE` from a full-table scan on
every pass, and M1 in §9 is the assumption it rests on.

### 8.3 Table properties

Set these in the three DDL files, so `make ddl-aws` creates a configured table
and no `ALTER TABLE` step is needed:

| Property | Value | Reason |
|---|---|---|
| `vacuum_max_snapshot_age_seconds` | `3600` | the 5-day default outlives the account; one hour of history is enough to debug a bad merge |
| `vacuum_min_snapshots_to_keep` | `5` | keeps a floor of history when the age cut-off would remove everything |
| `optimize_rewrite_data_file_threshold` | `5` (default) | 12 commits an hour reach 5 files in 25 minutes |
| `optimize_rewrite_delete_file_threshold` | `2` (default) | already aggressive; Gold needs it aggressive |

**Do not lower `vacuum_max_snapshot_age_seconds` below one hour.** A shorter
value removes the snapshot history you need to compare a bad Gold rebuild
against the one before it.

### 8.4 Observability

Add `awsnative/sql/verify_maintenance.sql`, in the pattern of the existing
`verify_silver_gold.sql`, with one query per number that decides whether
maintenance works:

- file count and average file size per partition, from `$partitions` and `$files`
- delete-file count per table
- snapshot count and oldest snapshot age, from `$snapshots`
- total file count before and after a pass

Expose it as `make maintenance-verify-aws`. Without these numbers the pass is
unverifiable, and an unverifiable maintenance job is the failure practice 9
describes.

### 8.5 Cost

`OPTIMIZE` is charged on bytes scanned. Each pass rewrites single-digit
megabytes across three tables, 24 passes a day at most. `VACUUM` adds S3
requests. The added Step Functions transitions are a few per hour. The total
belongs inside design §10's existing `~$4` Athena line and does not change the
`~$25 to $35/mo` total.

Apply the cadence lever from design §10 as well. A move from 5 to 15 minutes
cuts commits by two thirds, which reduces the cause rather than treating it. It
costs freshness, which is a stated trade-off, not a hidden one.

### 8.6 Named fallback

If this project ever gains a permanent bucket, switch to **Glue Data Catalog
table optimizers** (§4.2). They handle delete files, commit partial progress,
schedule themselves, and add sort and Z-order. The reason they lose today is the
$0.44 DPU-hour rate against an Athena pass that costs nothing when there is
nothing to do, plus the IAM role and the loss of per-partition control.

## 9. Assumptions to verify

Each assumption has a named fallback, so a false one is a course correction.

| # | Assumption | Verify with | Fallback if false |
|---|---|---|---|
| M1 | `OPTIMIZE ... WHERE` accepts a predicate on `event_ts` and `window_end_ts`, the source columns of the two `day()` transforms | run it against `silver_trades` | drop the `WHERE` clause and optimize the whole table; it is small |
| M2 | Athena's `$files` exposes a column that distinguishes data files from delete files | `DESCRIBE "db"."gold_bars_1m$files"` | count files from `$partitions` and infer delete pressure from query latency |
| M3 | A Step Functions `Choice` state can match the minute and the hour in `$$.Execution.StartTime` | one test execution | a second `aws_scheduler_schedule` at `rate(1 hour)`, plus an overlap guard that counts both state machines |
| M4 | Three `OPTIMIZE` statements plus the merges finish inside the 5-minute window | the execution duration in the state machine logs | move `VACUUM` and one `OPTIMIZE` to a separate hourly pass |
| M5 | The 96-commit estimate in §7.2 matches reality | `SELECT count(*) FROM "db"."silver_trades$snapshots"` | re-derive the cadence from the measured number |

M1 is the only one that changes the design. The rest degrade locally.

## 10. Rejected alternatives

- **Glue Data Catalog table optimizers now.** $0.44 per DPU-hour with a
  one-minute minimum, against an Athena pass that costs nothing when idle. It
  also needs a new IAM role and gives no per-partition control. Named as the
  fallback in §8.6 for a permanent account.
- **S3 Tables.** Managed maintenance, but it means a migration of three live
  tables, a second catalog, and a per-object plus per-GB cost model. The
  original rejection in design §13 stands.
- **Spark on Glue or EMR.** The only surface with `rewrite_manifests` and
  Z-order, and the wrong shape for three tables of single-digit megabytes. It
  stays the right answer for a large backlog.
- **A separate maintenance state machine on its own schedule.** Correct, and it
  needs a guard that counts executions of both state machines, or a
  cold-partition predicate that excludes the live window. Both add a
  conflict surface that the tail-state design does not have. Named as the M3
  fallback.
- **An S3 lifecycle rule on the table prefixes.** Would delete files that live
  snapshots reference and make the tables unreadable. Rejected on correctness,
  and recorded here because it looks like the cheap answer.
- **Copy-on-write for Gold, to avoid delete files entirely.** Not available.
  Athena ignores `write.merge.mode` and always uses merge-on-read.
- **Iceberg v3 deletion vectors.** They would reduce merge-on-read overhead
  directly. Athena engine v3 writes Iceberg v2 only and has no v3 support.
  Revisit when Athena ships it.

## 11. What this does not cover

- **Catalog choice.** Glue Data Catalog is assumed. Iceberg REST catalogs and
  Lake Formation governance are out of scope.
- **Schema and partition evolution.** A real concern for these tables and a
  separate decision.
- **Access control.** The point-in-time IAM boundary belongs to stage N5.
- **Disaster recovery.** Design §6 answers it with re-derivation.
- **Bronze.** `bronze_trades_stream` is plain Parquet with partition
  projection, not Iceberg. None of this applies to it.
- **Measured numbers.** Every number in §7.2 is derived from the schedule and
  the SQL. Section 9 gives the queries that replace derivation with measurement.

---

## Sources

- [Athena `OPTIMIZE`](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html)
- [Athena `VACUUM`](https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html)
- [Athena Iceberg table properties](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html)
- [Athena merge-on-read behaviour](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html)
- [Athena Iceberg optimization overview](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html)
- [Glue table optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
- [Glue compaction trigger threshold](https://docs.aws.amazon.com/glue/latest/dg/compaction-management.html)
- [Glue optimizer prerequisites (IAM)](https://docs.aws.amazon.com/glue/latest/dg/optimization-prerequisites.html)
- [Glue optimizer considerations and limitations](https://docs.aws.amazon.com/glue/latest/dg/optimizer-notes.html)
- [Glue pricing](https://aws.amazon.com/glue/pricing/)
- [AWS advanced automatic optimization announcement](https://aws.amazon.com/about-aws/whats-new/2024/12/aws-glue-data-catalog-automatic-optimization-iceberg-tables/)
- [AWS Prescriptive Guidance: compaction best practices](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/best-practices-compaction.html)
- [AWS: manage concurrent write conflicts in Iceberg](https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/)
- [S3 Tables maintenance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance-overview.html)
- [Terraform `aws_glue_catalog_table_optimizer`](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_table_optimizer)
