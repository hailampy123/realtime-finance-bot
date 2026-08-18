# AWS-Native Workstream — Stages N2 + N3 Run Guide

> **Everything these stages need already exists in your working tree** — the
> Terraform module, the SQL, the Python, the tests. Nothing has been staged or
> committed. This is a runbook: each task tells you which file to open, then
> which commands to run and what to expect back.
>
> **Committing is entirely up to you.** Nothing below tells you to commit.
> Suggested messages are collected at the end if you want a starting point.

**Goal:** Bronze trades merged into a deduplicated Silver Iceberg table with a
quarantine for violations, and rebuilt into 1-minute Gold bars — on a
five-minute schedule, with nothing silently dropped.

**Architecture:** One Step Functions state machine, three Athena statements, no
Lambda. An EventBridge schedule fires it every five minutes. Silver and
quarantine merge in parallel from the same Bronze window; Gold then rebuilds
only the partitions that window touched.

**Tech Stack:** Step Functions (Athena `.sync` and AWS SDK service
integrations), EventBridge Scheduler, Athena engine v3, Apache Iceberg on S3,
Glue Data Catalog.

**Spec:** [`2026-08-14-aws-native-workstream-design.md`](../specs/2026-08-14-aws-native-workstream-design.md)
— read §5.2, §5.3, §5.4 and D3 before starting. This guide covers stages N2 and
N3 of §12. Four places where the implementation refines the spec are recorded
in §14 of that document; the reasons are repeated where you meet them below.

**Prerequisite:** stage N1 complete, Bronze receiving trades. Run
`make verify-aws` after Task 4 only if `awsnative/sql/verify_bronze.sql` already
returns rows — everything here reads Bronze, so an empty Bronze produces empty
Silver and a stage gate that cannot fail.

---

## What's new in your repo

**SQL — one definition per transform, read by both Terraform and Python:**

| File | Responsibility |
|---|---|
| `awsnative/sql/fragments/valid_trade.sql` | The validity contract, as one boolean expression. Shared by both merges so they cannot drift apart. |
| `awsnative/sql/fragments/dirty_from_bronze.sql` | Which `(instrument_id, day)` partitions Gold must rebuild. |
| `awsnative/sql/ddl/010_silver_trades.sql` | Silver Iceberg table. |
| `awsnative/sql/ddl/020_silver_trades_quarantine.sql` | Quarantine Iceberg table, raw types preserved. |
| `awsnative/sql/ddl/030_gold_bars_1m.sql` | Gold Iceberg table, additive components only. |
| `awsnative/sql/merge_silver_trades.sql` | Bronze → Silver, insert-if-absent on `(venue, trade_id)`. |
| `awsnative/sql/merge_silver_quarantine.sql` | Bronze → quarantine, the complementary half. |
| `awsnative/sql/merge_gold_bars_1m.sql` | Silver → Gold, dirty partitions only. |
| `awsnative/sql/verify_silver_gold.sql` | The seven acceptance queries. |

**Python:**

| File | Responsibility |
|---|---|
| `awsnative/render.py` | Renders the `.sql` templates. The `${...}` syntax is shared with Terraform's `templatefile()`, which is why one copy serves both. |
| `awsnative/athena.py` | Minimal synchronous Athena client, plus a quote-aware statement splitter. |
| `awsnative/ddl.py` | `python -m awsnative.ddl` — creates the Iceberg tables. Idempotent. |
| `awsnative/query.py` | `python -m awsnative.query` — runs a `.sql` file and prints results with bytes scanned. |
| `awsnative/bars.py` | The additive-measure contract as executable Python. Nothing in production imports it; the tests do. |

**Tests:**

| File | Responsibility |
|---|---|
| `tests/awsnative/test_render.py` | Placeholder hygiene, and that the two predicates are literally `X` and `NOT X`. |
| `tests/awsnative/test_bars.py` | Grain invariance of VWAP, imbalance, volume, high/low — and the two measures that are deliberately *not* invariant. |
| `tests/awsnative/test_sql_contracts.py` | The DDL declares every component `bars.py` names, and stores no precomputed ratio. |
| `tests/awsnative/test_athena.py` | Polling, failure reporting, statement splitting. |

**Terraform and scripts:**

| Path | Responsibility |
|---|---|
| `infra/modules/native_medallion/` | Step Functions state machine, its IAM role, the EventBridge schedule. |
| `scripts/native_render_parity.sh` | Proves Terraform and Python render the same bytes. Offline. |

**New `make` targets:** `ddl-aws`, `microbatch-aws`, `verify-aws`, `sfn-logs-aws`.

**Where things stand:** `make check` is green — 285 passed, 7 skipped, lint,
format and mypy clean. `./scripts/native_render_parity.sh` passes. Nothing
Terraform-shaped has been run: no `plan`, no `apply`, no `aws` call that changes
anything. Those are yours.

---

## How this stage differs from N0/N1

**No `-target`.** N0/N1 used it to get a piece-by-piece progression back from
already-written code. That does not apply here: N2 and N3 are one state machine
and three tables, and there is no honest way to half-apply a single resource.

**They deploy together and verify separately.** You will apply everything once,
then meet the N2 gate (Tasks 6–7) before looking at Gold at all. §12's
"independently verifiable" property survives; "independently deployable" does
not, and pretending otherwise would mean building a one-branch state machine
and then editing it.

**The schedule starts disarmed.** `microbatch_enabled = false` in your tfvars.
You will trigger the first executions by hand and read them. A failure you
caused is much easier to understand than one that arrived on a timer while you
were reading something else.

---

## Task 1: Read the three transforms

Nothing to run. This is the stage where the interesting decisions live in SQL
rather than in Terraform, so read the SQL first — every file leads with the
reasoning, and the comments are the lesson.

**Read, in this order:**

1. `awsnative/sql/fragments/valid_trade.sql` — the validity contract. Note the
   `COALESCE` note: Firehose writes `NULL` for any JSON key the Glue schema does
   not name, and `NULL` fails both a predicate *and* its negation, so without
   the guard a malformed row would land in neither table. That is the silent
   drop §5.4 forbids, and it is the single subtlest thing in this stage.

2. `awsnative/sql/merge_silver_trades.sql` — insert-if-absent, no `UPDATE`
   branch, no lateness cutoff. Then find the `row_number()` subquery and read
   why it is not optional: `MERGE`'s `NOT MATCHED` protects against a duplicate
   already in the target and does nothing about two duplicates arriving in the
   same batch.

3. `awsnative/sql/merge_silver_quarantine.sql` — the complementary half. It is a
   `MERGE`, not the `INSERT` §5.4 describes, because the Bronze window overlaps
   between runs and an `INSERT` would re-add the same bad row 288 times a day.
   Note `row_key`: a row can be quarantined *for* having a `NULL` venue, and
   `NULL = NULL` is never true, so the obvious key would never match.

4. `awsnative/sql/ddl/030_gold_bars_1m.sql` — the table that stores `notional`
   and `volume` but no `vwap`. Read the non-additive exception for `open` and
   `close`.

5. `awsnative/sql/merge_gold_bars_1m.sql` — one `MERGE`, one Iceberg commit,
   dirty partitions only. Note the tie-break on `ROW(event_ts_us, trade_id)`:
   several trades routinely share a millisecond, and without it a re-run could
   write a different `open` for the same bar.

**Then read** `infra/modules/native_medallion/main.tf` — specifically the
`locals` block at the top, which renders those files into the state machine, and
the `definition` further down.

---

## Task 2: Watch the offline guards work

These run with no AWS credentials. Run them, then break one on purpose — a
tripwire you have not seen fail is a tripwire you do not know works.

**Step 1: the full check.**

```bash
make check
```

Expected: `285 passed, 7 skipped`, lint/format/mypy clean.

**Step 2: the render parity check.**

```bash
./scripts/native_render_parity.sh
```

Expected:

```
  merge_gold_bars_1m.sql           identical (6957 chars)
  merge_silver_quarantine.sql      identical (5780 chars)
  merge_silver_trades.sql          identical (5081 chars)

Renderers agree.
```

This is the only thing that checks that the SQL in the state machine is the SQL
the tests assert against. `terraform validate` does not evaluate
`templatefile()`, so without this the merge SQL is unchecked until apply.

**Step 3: break the complementarity guard and watch it catch you.**

Open `awsnative/sql/merge_silver_quarantine.sql` and change

```sql
  AND NOT COALESCE(${valid_expr}, false)
```

to

```sql
  AND NOT (${valid_expr})
```

— a change that looks equivalent and quietly reintroduces the silent drop.

```bash
uv run --group awsnative pytest tests/awsnative/test_render.py -q
```

Expected: `test_valid_and_invalid_predicates_are_exact_complements` and
`test_coalesce_wraps_the_predicate_on_both_sides` both fail.

**Step 4: revert.**

```bash
git checkout -- awsnative/sql/merge_silver_quarantine.sql
uv run --group awsnative pytest tests/awsnative/test_render.py -q
```

Expected: all pass.

---

## Task 3: Deploy the state machine

**Concepts.** Step Functions' **service integrations** let a state call an AWS
API directly, with no Lambda in between. Two kinds appear here. The `.sync`
suffix on `arn:aws:states:::athena:startQueryExecution.sync` means "start it and
block until it finishes" — Step Functions polls Athena for you, which is the
entire reason this stage needs no code deployed anywhere. The
`arn:aws:states:::aws-sdk:sfn:listExecutions` form is the generic **AWS SDK
integration**: any API of any service, called directly from a state.

The overlap guard uses the second form to ask how many executions of itself are
running. `$$` (double dollar) reads the **context object** — runtime metadata
about the execution rather than its data — so `$$.StateMachine.Id` is the state
machine's own ARN without Terraform having to inject it.

**Step 1: set the schedule disarmed.**

```bash
cp infra/envs/native/terraform.tfvars.example infra/envs/native/terraform.tfvars
```

If you already have a `terraform.tfvars` from N1, just add:

```hcl
microbatch_enabled = false
```

**Step 2: plan.**

```bash
terraform -chdir=infra/envs/native init -input=false
terraform -chdir=infra/envs/native plan
```

Expected: **6 to add, 0 to change, 0 to destroy** — two IAM roles, two role
policies, a log group, a state machine, a schedule. (Seven if you count both
roles' policies separately; the exact number is less interesting than the
absence of anything to *destroy*. If Terraform wants to destroy N1 resources,
stop and read why before continuing.)

**Step 3: apply.**

```bash
terraform -chdir=infra/envs/native apply
```

**Step 4: look at what you built.** Open the Step Functions console → State
machines → `fdai-native-microbatch` → the **Definition** tab. The graph is the
lesson: a task, a choice, a parallel with two branches, a task. Click
`MergeSilver` and read the `QueryString` — that is your `.sql` file, rendered.

```bash
terraform -chdir=infra/envs/native output microbatch_state_machine_arn
terraform -chdir=infra/envs/native output microbatch_schedule
```

**If apply fails with `Invalid State Machine Definition`**, the message names
the state and the problem — a `Next` pointing at a state that does not exist is
the usual cause, and it is caught here rather than at run time.

---

## Task 4: Create the Iceberg tables

**Concepts.** These three tables cannot be Terraform resources. `silver_trades`
is `PARTITIONED BY (instrument_id, day(event_ts))` — `day()` is an **Iceberg
partition transform**, and Glue's `CreateTable` API accepts identity partitions
only. There is no way to configure `aws_glue_catalog_table` into expressing it.
So the DDL goes through Athena, and `make up-aws` sends it.

That costs the letter of §8: `terraform apply` alone no longer produces the
whole stage. `make up-aws` still does, and `docker build`/`push` has stood
outside Terraform since N1 for the same reason — some things are not resources.

**Step 1: read the DDL that is about to run, without running it.**

```bash
uv run --group awsnative python -m awsnative.ddl \
  --database fdai_native --workgroup fdai-native --bucket example --dry-run
```

**Step 2: create them.**

```bash
make ddl-aws
```

Expected:

```
==> 010_silver_trades.sql
    ok (1234 ms)
==> 020_silver_trades_quarantine.sql
    ok (987 ms)
==> 030_gold_bars_1m.sql
    ok (1102 ms)

Tables ready. Verification queries: awsnative/sql/verify_silver_gold.sql
```

**Step 3: run it again.** It is idempotent — that matters, because
`make up-aws` runs it on every bring-up.

**Step 4: look at what a new Iceberg table actually is.**

```bash
aws s3 ls "s3://$(terraform -chdir=infra/envs/native output -raw lake_bucket)/silver_trades/" --recursive
```

Expected: **only `metadata/`**, holding one `.metadata.json`. No data files
exist yet. An Iceberg table is a pointer in Glue to a metadata file that lists
snapshots; a commit writes a new metadata file and atomically swings the
pointer. That is the whole mechanism, and it is why `MERGE` is atomic and why
the state machine's role needs `glue:UpdateTable`.

In the Athena console, run `DESCRIBE silver_trades` and confirm the partition
columns are there.

---

## Task 5: Trigger one micro-batch by hand

**Step 1: run it, and wait.**

```bash
make microbatch-aws
```

Expected:

```
started arn:aws:states:...:execution:fdai-native-microbatch:...
finished: SUCCEEDED
```

**Step 2: read the execution.** Step Functions console → the state machine →
**Executions** → the newest one. Work through the graph view:

- `CountRunningExecutions` returned `{"running": 1}` — itself.
- `AlreadyRunning` took the **Default** branch, because `1 > 1` is false.
- `SilverAndQuarantine` ran two branches at once. Click each; the output carries
  the Athena `QueryExecutionId`.
- `MergeGold` ran after both finished.

**Step 3: find what those queries cost.** Athena console → **Query editor** →
**Recent queries**. You should see three (or four — Athena logs the DDL too).
Look at the **Data scanned** column.

Expected: **single-digit to low-double-digit MB**, not GB. This is the number
that decides whether §10's cost model holds. Bronze uses partition projection,
and the merges filter on `ingest_date`, so Athena should be reading two day
prefixes rather than the table.

> **If Data scanned is gigabytes**, projection is not pruning. Check that the
> `ingest_date` predicate survived rendering — click into the query text and
> confirm you see a literal `>= '2026-08-15'`-shaped comparison after Athena
> constant-folds `current_date`. This is the assumption in this stage most worth
> distrusting, which is why it has its own step.

**Step 4: run it twice more.**

```bash
make microbatch-aws && make microbatch-aws
```

Both must succeed. The merges are idempotent — that is not a nice-to-have, it is
what lets the state machine retry blindly and what makes an overlapping Bronze
window safe.

---

## Task 6: The N2 gate

```bash
make verify-aws
```

That runs all seven queries. The two that are the gate:

**Query 2 — the immutability tripwire. Must return zero rows.** Silver has no
`UPDATE` branch because a trade is an immutable fact: the stream copy and the
archive copy of the same trade should agree on `event_ts_us`, `price` and
`size`. If they do not, that assumption is broken, and it has to be found here
rather than by a backtest that quietly double-counts volume. **A non-zero result
is a finding, not a warning to suppress.**

**Query 3 — nothing silently dropped. `unaccounted` must be 0.** Every distinct
`(venue, trade_id)` that reached Bronze is in exactly one of `silver_trades` or
`silver_trades_quarantine`.

> **A non-zero `unaccounted` is expected if Bronze received rows after your last
> execution.** Re-run `make microbatch-aws`, wait, and re-check before believing
> it. If it stays non-zero with no new Bronze arriving, that is the real thing:
> the `COALESCE` guard is the first place to look.

**Query 1** should show both venues with a recent `last_event`. **Query 4**
should be empty or near-empty on live stream data — a quarantine rate above a
fraction of a percent means the encoder, the Glue schema and the validity
contract disagree, and the reasons name which conjunct failed.

---

## Task 7 (optional): Prove the quarantine path with a bad row

Query 4 returning nothing is ambiguous: it means either "no bad data" or "the
quarantine branch never runs". Worth resolving, and it costs one row.

**This leaves a permanent bad row in Bronze and a permanent row in quarantine.**
Bronze is an external Parquet table, so there is no `DELETE`. Both disappear
with the weekly wipe or `make down-aws`. Skip this task if that bothers you.

**Step 1: write one deliberately invalid row into Bronze.** In the Athena
console, against the `fdai-native` workgroup and `fdai_native` database:

```sql
INSERT INTO bronze_trades_stream
SELECT 'binance', 'TESTUSDT', 'TEST-USD', 'quarantine-probe-1',
       1750000000000000, 1750000000000000,
       '-1', '1', 'BUY', 1, false, 'STREAM', 1,
       date_format(current_date, '%Y-%m-%d');
```

A negative price. Nothing else about the row is wrong, so it isolates one
conjunct of the validity contract.

**Step 2: merge and look.**

```bash
make microbatch-aws
```

Then in Athena:

```sql
SELECT trade_id, price, quarantine_reason, quarantined_ts
FROM silver_trades_quarantine
WHERE trade_id = 'quarantine-probe-1';

SELECT count(*) FROM silver_trades WHERE trade_id = 'quarantine-probe-1';
```

Expected: one quarantine row with `quarantine_reason = 'price'`, and **zero**
rows in Silver.

**Step 3: prove it is idempotent.** Run `make microbatch-aws` again, then
re-count the quarantine table.

Expected: still exactly one row. This is what the `row_key` merge buys — with
the `INSERT` §5.4 literally describes, you would now have two, then three, then
288 a day, and the quarantine rate would be measuring the schedule rather than
the data.

---

## Task 8: The N3 gate

**Query 5** should show bars per instrument with `minutes_behind` in the
single digits. Firehose buffers 120s and then the merge runs, so a few minutes
is correct, not a problem.

**Query 6 is the gate: `vwap_abs_diff` and `imbalance_abs_diff` must be
below ~1e-9.** It rolls the 1-minute bars up to 5 minutes from the stored
components and computes the same measures directly from `silver_trades`. They
must agree. If they do not, Gold is storing something it should not — most
likely a ratio.

Notice what query 6 does *not* compare: OHLC. `open` and `close` are first/last
by event time and cannot be re-derived at a coarser grain, which is why read
paths expose OHLC at 1-minute grain only (§5.3). That omission is the design,
stated.

**Query 7 shows you why**, on your own data: `naive_vwap` averages the
per-minute VWAPs, `correct_vwap` divides the summed components, and `error` is
what a `vwap` column in Gold would have cost you. It is close enough to look
right and wrong enough to matter.

**Also worth doing once:** in the Athena console, run

```sql
SELECT * FROM "fdai_native"."gold_bars_1m$snapshots" ORDER BY committed_at DESC LIMIT 10;
```

Iceberg exposes its own metadata as queryable tables. One row per commit — so
one per micro-batch execution that changed anything. This is the audit trail
that makes "re-running a failed step is safe" checkable rather than asserted.

---

## Task 9: Arm the schedule

**Step 1:** set `microbatch_enabled = true` in
`infra/envs/native/terraform.tfvars`, then:

```bash
terraform -chdir=infra/envs/native apply
```

Expected: **1 to change** — the schedule's `state`.

Managing this through Terraform rather than clicking *Disable* in the console
is the point: the console and the state file never disagree.

**Step 2: watch it run on its own.**

```bash
make sfn-logs-aws
```

Leave it for ten minutes. You should see two executions start, five minutes
apart.

**Step 3: verify freshness improved.** Re-run `make verify-aws` and look at
`minutes_behind` in query 5. It should now stay under about seven minutes
without you doing anything.

---

## Task 10: Cost, checked rather than assumed

**Step 1: total the scan.** Athena console → Recent queries. Add up **Data
scanned** across one micro-batch's three statements.

At 288 executions/day, daily Athena spend ≈ `3 × MB_per_statement × 288 / 1024 × $5/TB`.
A few MB per statement lands around a few cents a day *while the stack is up*.
§10's ~$4/month for Athena assumes ~130 h/month of operation, not 24/7 — that
operating discipline is doing real work in the budget, which is worth knowing
before leaving this running over a weekend.

**Step 2: the levers, in order of effect.**

| Lever | How | Cost of pulling it |
|---|---|---|
| Run fewer hours | `make down-aws` when not working | none — this is the intended discipline |
| Lengthen the cadence | `microbatch_schedule = "rate(15 minutes)"` | ~⅔ off Athena; breaks the p50 < 6 min freshness SLO |
| Narrow the window | `microbatch_lookback_days = 0` | today only; a run spanning UTC midnight misses yesterday's tail until the next day's run |
| Partition Bronze hourly | change the Firehose prefix and the projection | the big one *if* you ever run continuously; unnecessary at 130 h/month, and it would mean rewriting working N1 infrastructure |

The last row is a decision that was made and then unmade during this build:
hourly Bronze partitions look obviously right until you check them against the
actual operating hours, at which point they save nothing and cost a migration.

---

## Task 11: The full round trip

**Step 1: destroy.**

```bash
make down-aws
```

**Step 2: rebuild from empty.**

```bash
make up-aws
```

Watch for the new step: `==> creating the Iceberg tables`, between
`terraform apply` and the image build.

**Step 3: prove it.**

```bash
make microbatch-aws
make verify-aws
```

Silver and Gold are re-derived from whatever Bronze the producer has managed to
land since bring-up. History before the destroy is gone — that is §6's
durability model, and stage N4 is what makes it recoverable.

---

## Stage N2 — Definition of Done

- [ ] `silver_trades` and `silver_trades_quarantine` exist as Iceberg tables
- [ ] Query 2 returns zero rows (immutability holds)
- [ ] Query 3 returns `unaccounted = 0` (nothing silently dropped)
- [ ] Both venues appear in query 1
- [ ] A deliberately invalid row lands in quarantine, not Silver, and lands
      exactly once no matter how many times the merge runs (Task 7)
- [ ] Re-running the micro-batch changes no counts

## Stage N3 — Definition of Done

- [ ] `gold_bars_1m` has bars for every instrument with recent trades
- [ ] Query 6: `vwap_abs_diff` and `imbalance_abs_diff` below 1e-9
- [ ] `minutes_behind` stays under ~7 with the schedule armed
- [ ] Query 7 shows a non-zero `error` — the mistake the design avoids, on your
      own data
- [ ] `terraform apply` after `make down-aws` reproduces both stages

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AccessDeniedException` on `glue:UpdateTable` during a merge | Iceberg commits through Glue; the merge wrote data files then could not publish them | The role policy is in `native_medallion/main.tf` → `ReadAndCommitIcebergMetadata`. Re-apply. |
| Merge fails on `s3:DeleteObject` | Iceberg rewrites data files and expires the old ones | Same policy, `ReadAndWriteTheLake`. Easy to miss because a read-only merge would never hit it. |
| `Data scanned` in GB | Partition projection not pruning | Confirm the `ingest_date` predicate is present in the rendered query. Fallback: interpolate literal partition values from the state machine. |
| `MERGE` rejected as unsupported | Athena engine v2 | Workgroup must be engine v3 (spec §11 A2). Fallback there is `DELETE` + `INSERT` per dirty partition. |
| Executions overlap and one fails on an Iceberg commit conflict | A merge took longer than the schedule | Expected and handled — `CountRunningExecutions` skips. If it happens constantly, lengthen the cadence. |
| `Invalid State Machine Definition` at apply | A `Next` naming a state that does not exist | The message names the state. |
| Gold bars appear then change values on re-run | `open`/`close` tie-break removed | `min_by(price, ROW(event_ts_us, trade_id))` — the `ROW` is what makes the choice total. |
| `min_by`/`max_by` rejects the `ROW(...)` key | Engine version does not treat `ROW` as orderable | Swap both for `element_at(array_agg(s.price ORDER BY s.event_ts_us ASC, s.trade_id ASC), 1)` (and `DESC` for close). Same total order, more memory per group. |
| Quarantine row count grows with no new Bronze | A row with a `NULL` natural key, or the merge reverted to `INSERT` | Query 4's `null_key` reason. See the `row_key` note in the DDL. |
| `make verify-aws` fails on `terraform output` | Stack destroyed, or wrong AWS profile | `terraform -chdir=infra/envs/native output` to confirm. |

---

## What comes next

**N4 — backfill.** The deep tier (2 y of 1 m klines) merges straight into
`gold_bars_1m` with `source_tier = ARCHIVE_KLINE`; the hot tier (aggTrades)
merges into `silver_trades`. Two things in this stage were built for it:
`merge_gold_bars_1m.sql` takes its dirty set as a rendered fragment, so N4 swaps
in `dirty_from_staging.sql` and reuses the bars computation unchanged (that is
D3's real invariant); and N4's Gold merge needs a guard this one deliberately
omits — `ARCHIVE_KLINE` must not overwrite `DERIVED_FROM_TRADES`, because it is
the lower-fidelity source. Reconciliation belongs to N4, not here: with no
archive klines to compare against, it would produce exactly the vacuous green
check §6.4 warns about.

**N5 — prepared statements and the IAM boundary.** Where the deploy-time /
run-time placeholder distinction in `render.py` starts to earn its keep: `*_pit`
statements take a real `?` parameter bound by Athena, not substituted by string
rendering, and the tool-server role becomes the only principal that can read
Gold.

---

## Suggested commit messages (optional, for when you're ready)

Reference material, not instructions. Split or squash however you like.

```
feat(native): add the Silver merge, quarantine split and validity contract

Bronze -> Silver as insert-if-absent on (venue, trade_id): a trade is an
immutable fact, so there is no UPDATE branch and no lateness cutoff. The
validity predicate lives in one fragment shared by both merges, wrapped in
COALESCE on one side and NOT COALESCE on the other, so a row can never be
rejected by both -- Firehose writes NULL for unmatched keys and NULL fails a
predicate and its negation alike.

Quarantine is a MERGE rather than the INSERT the spec describes: the Bronze
window overlaps between runs, so an INSERT would re-add the same bad row 288
times a day. It is keyed on a hash of the raw tuple because a row can be
quarantined for having a NULL natural key.
```

```
feat(native): add gold_bars_1m with additive components only

Stores notional and volume, never vwap; buy_vol and sell_vol, never imbalance.
A ratio is correct at exactly one grain and quietly wrong at every other.
bars.py states the contract in Python and test_sql_contracts.py asserts the DDL
declares every component it names.

open and close are the one documented non-additive exception and are tie-broken
on ROW(event_ts_us, trade_id): several trades share a millisecond at crypto
rates, and without a total order a re-run writes a different open for the same
bar.
```

```
feat(native): add the micro-batch state machine and its schedule

One Step Functions execution owns the Silver merge and the Gold rebuild as
consecutive states, which is why nothing here needs a Change Data Feed to
discover what moved (spec D3). Silver and quarantine run in parallel: different
tables, both idempotent, so a half-failed parallel retries clean.

An overlap guard reads its own execution list through the AWS SDK integration
and skips rather than queueing -- the next tick reads the same window, so
nothing is missed by not running now.
```

```
feat(native): create the Iceberg tables through Athena, not Terraform

Glue's CreateTable API accepts identity partitions only and cannot express
day(event_ts) however aws_glue_catalog_table is configured. The DDL therefore
goes through Athena as a step in make up-aws, alongside docker build/push,
which has stood outside Terraform since N1 for the same reason.
```

```
test(native): prove Terraform and Python render the same SQL

awsnative/sql holds one copy of each transform read by two renderers that
cannot see each other. terraform validate does not evaluate templatefile(), so
without this check the SQL in the state machine is unverified until apply -- and
a stray .strip() on the Python side is enough to make the tested SQL differ from
the deployed SQL.
```
