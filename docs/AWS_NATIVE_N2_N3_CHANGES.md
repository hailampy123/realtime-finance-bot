# Stages N2 + N3 — what changed

A plain inventory of what stages N2 (Silver + quarantine) and N3 (Gold bars)
add to the repo and to the AWS account, and the handful of places where the
implementation differs from the design.

Run guide: [`superpowers/plans/2026-08-16-aws-native-n2-n3.md`](superpowers/plans/2026-08-16-aws-native-n2-n3.md)
Design: [`superpowers/specs/2026-08-14-aws-native-workstream-design.md`](superpowers/specs/2026-08-14-aws-native-workstream-design.md)

## In one picture

```
             ── N0/N1, unchanged ──────────────────  ── N2 ──────────────  ── N3 ──
Binance WS ┐
Coinbase WS┴→ Fargate → Kinesis → Firehose → Bronze ─┬→ silver_trades ─────→ gold_bars_1m
                                        (Parquet)    └→ silver_trades_quarantine

                        Step Functions, every 5 min, one execution:
                          [ merge Silver ‖ merge quarantine ] → merge Gold
```

Nothing in the ingest path changed. N2/N3 sit entirely downstream of Bronze.

## New AWS services

Three services this project had not used before N2:

| Service | What it does here | Always-on cost |
|---|---|---|
| **AWS Step Functions** | Runs the three Athena statements in order, with retries and an overlap guard. Standard workflow, ~5 state transitions per execution. | none — per transition |
| **EventBridge Scheduler** | Fires one execution every five minutes. | none |
| **Apache Iceberg (via Athena)** | Table format for Silver, quarantine and Gold. Gives `MERGE INTO`, atomic commits and snapshot history on plain S3. | none — it is a file layout plus Glue metadata |

Services already in use that N2/N3 lean on harder: **Athena** (now runs the
transforms, not just ad-hoc queries), **Glue Data Catalog** (Iceberg commits are
`UpdateTable` calls against it), **S3**, **IAM**, **CloudWatch Logs**.

Still not used: Lambda. The `.sync` Athena service integration means Step
Functions polls Athena directly, so there is no code deployed anywhere in this
stage. Lambda arrives in N4 for archive I/O.

## New AWS resources

Seven, all in `infra/modules/native_medallion/`:

| Resource | Name |
|---|---|
| `aws_sfn_state_machine` | `fdai-native-microbatch` |
| `aws_iam_role` + policy | `fdai-native-microbatch` (Athena, Glue, S3, its own execution list, log delivery) |
| `aws_iam_role` + policy | `fdai-native-microbatch-scheduler` (`states:StartExecution`, nothing else) |
| `aws_cloudwatch_log_group` | `/aws/vendedlogs/states/fdai-native-microbatch` |
| `aws_scheduler_schedule` | `fdai-native-microbatch` |

Plus three Glue tables that are **not** Terraform resources — see "Deviations"
below.

## New data

| Table | Format | Partitioned by | Key | Grows with |
|---|---|---|---|---|
| `silver_trades` | Iceberg | `(instrument_id, day(event_ts))` | `(venue, trade_id)` | every distinct valid trade |
| `silver_trades_quarantine` | Iceberg | `(ingest_date)` | `row_key` (hash of the raw tuple) | every distinct invalid trade |
| `gold_bars_1m` | Iceberg | `(instrument_id, day(window_end_ts))` | `(instrument_id, window_end_ts)` | one row per instrument-minute |

All three live under the existing lake bucket, `s3://fdai-native-lake-<account>/`.
All three are re-derivable and go away with `make down-aws`.

**What Gold deliberately does not store:** `vwap`, `flow_imbalance`,
`realized_vol`. It stores `notional`, `volume`, `buy_vol`, `sell_vol`,
`sq_log_return` and lets the reader divide. A stored ratio is correct at exactly
one grain and quietly wrong at every other.

**The one non-additive exception:** `open` and `close` are first/last by event
time and cannot be re-derived when rolling 1-minute bars up to 5. Read paths
expose OHLC at 1-minute grain only.

## New files

**SQL** — `awsnative/sql/`. One definition per transform, read by both
Terraform (which bakes it into the state machine) and Python (tests, manual
runs).

```
fragments/valid_trade.sql          the validity contract, one boolean expression
fragments/dirty_from_bronze.sql    which partitions Gold must rebuild
ddl/010_silver_trades.sql
ddl/020_silver_trades_quarantine.sql
ddl/030_gold_bars_1m.sql
merge_silver_trades.sql
merge_silver_quarantine.sql
merge_gold_bars_1m.sql
verify_silver_gold.sql             seven acceptance queries
```

**Python** — `awsnative/`

| File | What it is |
|---|---|
| `render.py` | Renders the `.sql` templates. Uses `string.Template` specifically because its `${...}` syntax matches Terraform's `templatefile()`. |
| `athena.py` | Synchronous Athena client (start, poll, fetch) and a quote-aware statement splitter. |
| `ddl.py` | `python -m awsnative.ddl` — creates the Iceberg tables. Idempotent. |
| `query.py` | `python -m awsnative.query` — runs a `.sql` file, prints results and bytes scanned. |
| `bars.py` | The additive-measure contract as executable Python. Not imported by anything in production; it exists so a test can check the DDL against it. |

**Tests** — `tests/awsnative/`: `test_render.py`, `test_bars.py`,
`test_sql_contracts.py`, `test_athena.py`. 93 tests across the `awsnative`
package, up from 18.

**Terraform** — `infra/modules/native_medallion/{main,variables,outputs}.tf`

**Scripts** — `scripts/native_render_parity.sh`

## Changed files

| File | Change |
|---|---|
| `infra/envs/native/main.tf` | New `module "medallion"` block |
| `infra/envs/native/variables.tf` | `microbatch_schedule`, `microbatch_enabled`, `microbatch_lookback_days` |
| `infra/envs/native/outputs.tf` | Four `microbatch_*` outputs |
| `infra/envs/native/terraform.tfvars.example` | `microbatch_enabled = false` for the first run |
| `scripts/native_up.sh` | New DDL step between `terraform apply` and the image build |
| `Makefile` | `ddl-aws`, `microbatch-aws`, `verify-aws`, `sfn-logs-aws`; `validate-aws` now runs the parity check |

## New commands

```bash
make ddl-aws          # create the Iceberg tables (idempotent)
make microbatch-aws   # run one merge cycle now and wait for it
make verify-aws       # the seven acceptance queries, with bytes scanned
make sfn-logs-aws     # follow the state machine's execution logs
make validate-aws     # offline: terraform validate + fmt + render parity
```

## Deviations from the design

Recorded in full as §14 of the design doc. In brief:

| # | Design said | Implementation does | Why |
|---|---|---|---|
| A1 | Quarantine is an `INSERT` (§5.4) | A `MERGE` keyed on a row hash | The window overlaps between runs; an `INSERT` re-adds the same bad row 288×/day |
| A2 | Two complementary predicates (§5.4) | The same, `COALESCE`-guarded on both sides | `NULL` fails a predicate *and* its negation, so a malformed row would land in neither table |
| A3 | Dirty set passed between states (D3) | Derived as a CTE from the same Bronze window | `MERGE` reports no affected rows. D3's real invariant — one shared bars computation — is preserved as a rendered fragment |
| A4 | `terraform apply` reproduces a stage (§8) | `make up-aws` does; `terraform apply` does not create the Iceberg tables | Glue's `CreateTable` cannot express `day(event_ts)` under any configuration |
| A5 | — | The state machine skips if one is already running | Concurrent Iceberg merges pay twice and fail on the commit lock |
| A6 | Bronze partitioned daily (§5.1) | Unchanged — but deliberately, having checked | Hourly looks right until measured against §10's ~130 h/month; then it saves nothing and costs a rewrite of live N1 infrastructure |

## Cost

Added to §10's estimate: Step Functions (~1,700 transitions/day at $0.025 per
1,000 = well under a dollar a month at this duty cycle), EventBridge Scheduler
(free at this volume), and the Athena the merges consume — a few MB per
statement, three statements per execution.

The estimate is unchanged in shape from §10 and, like §10, assumes ~130 h/month
of operation rather than 24/7. That assumption is doing real work; leaving the
schedule armed over a long weekend is the single easiest way to be surprised by
a bill. Verify with the "Data scanned" column in the Athena console rather than
trusting the arithmetic — that step is Task 10 of the run guide.

## What this does not do yet

- **No history.** Silver and Gold hold whatever Bronze has landed since the last
  bring-up. Stage N4's backfill is what makes the weekly wipe survivable.
- **No reconciliation.** It needs archive klines to compare stream-derived bars
  against, and belongs to N4 — attempting it now would produce a check that
  cannot fail, which §6.4 argues is worse than no check.
- **No cross-venue price sanity check.** Deferred in §5.4; needs a window over
  other venues.
- **No anti-lookahead boundary.** Gold is readable by anything with Athena
  access. Stage N5 adds the prepared statements and the IAM scoping that make
  the point-in-time guarantee real.
