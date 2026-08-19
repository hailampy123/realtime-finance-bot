# Data layer

Every table this project stores, in both workstreams: where the bytes live,
how it is partitioned, what one row means, which process writes it, on what
schedule, how fresh it is, and roughly how big it gets.

**Why this layer exists.** The trading-decision agent (stage N6, design-only)
needs Bronze/Silver/Gold history it can trust: point-in-time correct,
re-derivable after the weekly wipe, and cheap enough to run continuously.
Everything below serves that goal. The enrichment tables are informational
first; a hard trading constraint gets added only after a backtest earns it
(see [`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md) §5).

Read this to query the data, add a table, or check whether something is
actually running. For the processes that fill these tables, see
[`ARCHITECTURE.md`](ARCHITECTURE.md). For what to build next, see
[`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md).

---

## 1. Two workstreams, one ingestion contract

The repo implements the same use case twice. Both stacks read the same exchange
feeds through the same Python code, then diverge at the sink.

| Property | AWS-native | Kafka / Databricks |
|---|---|---|
| Shared code | `ingest/`, `config/universe.yaml`, `ingest/schemas/trade.v1.avsc` | the same three |
| Transport | Kinesis Data Streams | MSK (Kafka) |
| Landing | Firehose to Parquet on S3 | Structured Streaming to Delta |
| Storage format | Iceberg on plain S3 | Unity Catalog managed Delta |
| Query engine | Athena | Databricks SQL / Spark |
| Transform code | Athena SQL, run by Step Functions | PySpark, run by a Lakeflow pipeline |
| Catalog | Glue database `fdai_native` | catalog `fdai`, schema `market` |
| Terraform env | `infra/envs/native` | `infra/envs/dev` |

Both stacks can run at the same time. They use separate VPCs, separate
Terraform state keys, and the `fdai-native-*` prefix keeps their resource names
apart.

## 2. The durability model decides the layout

The AWS sandbox account is wiped every 7 days. Each workstream answers that
differently, and the answer drives every storage decision below.

**Kafka / Databricks path.** Unity Catalog managed Delta lives in the permanent
Databricks account. It survives the wipe and is the system of record. The
sandbox holds only compute and the Kafka brokers.

**AWS-native path.** Nothing survives. Durability comes from re-derivation:
after a wipe, `make up-aws` recreates the tables and stage N4's backfill
reloads history from the Binance public archive.

One rule follows from re-derivation. Every AWS-native table must be rebuildable
from a public archive or from another table in the same lake. A source with no
durable, free history fails that test, which is why on-chain metrics are
rejected and news carries a forward-only caveat. See
[`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md) §2.4 and §2.5.

## 3. AWS-native storage layout

One bucket holds the whole lake. Athena results and Firehose error records
expire on a lifecycle rule; nothing else does.

```text
s3://fdai-native-lake-<account-id>/
├── bronze_trades_stream/ingest_date=YYYY-MM-DD/      Firehose, Parquet
├── bronze_perp_context/ingest_date=YYYY-MM-DD/       Lambda, JSON
├── bronze_macro_observations/ingest_date=YYYY-MM-DD/ Lambda, JSON
├── silver_trades/                                    Iceberg
├── silver_trades_quarantine/                         Iceberg
├── silver_perp_context/                              Iceberg
├── silver_macro/                                     Iceberg
├── gold_bars_1m/                                     Iceberg
├── backfill_manifest/                                Iceberg
├── archive_staging_klines/                           CSV, transient
├── archive_staging_trades/                           CSV, transient
├── _backfill/outcomes/                               JSON, loader output
├── _athena-results/                                  expires on a lifecycle rule
└── _errors/                                          Firehose failures, expires
```

Glue database: `fdai_native`. Athena workgroup: `fdai-native`, with
`enforce_workgroup_configuration = true` so every query writes results to the
prefix above.

The three Bronze tables use **partition projection**, not a partition
registry. Athena computes each partition location from
`storage.location.template` rather than reading a list from Glue. No crawler
runs, and the catalog cannot disagree with S3 about which partitions exist.
Projection starts at `projection_start_date` (default `2026-01-01`); a query
for an earlier date returns nothing rather than an error.

### File types and table configuration

One row per group of tables that share a format and a set of settings.

| Tables | File format | Compression | Glue table type | Partition mechanism |
|---|---|---|---|---|
| `bronze_trades_stream` | Parquet | Snappy | `EXTERNAL_TABLE` | partition projection on `ingest_date` |
| `bronze_perp_context`, `bronze_macro_observations` | JSON (`JsonSerDe`) | none | `EXTERNAL_TABLE` | partition projection on `ingest_date` |
| `silver_trades`, `silver_trades_quarantine`, `silver_perp_context`, `silver_macro`, `gold_bars_1m`, `backfill_manifest` | Parquet under Iceberg | Snappy | `table_type = ICEBERG` | Iceberg partition transform, for example `day(event_ts)` or a plain column |
| `archive_staging_klines`, `archive_staging_trades` | CSV, `STORED AS TEXTFILE` | none | `EXTERNAL_TABLE` | none, transient |
| `backfill_outcomes` | JSON (`JsonSerDe`) | none | `EXTERNAL_TABLE` | none |

The Iceberg partition transform is why Terraform cannot create those six
tables: Glue's `aws_glue_catalog_table` `CreateTable` API has no way to express
`day(event_ts)`. Only `bronze_trades_stream` is a Terraform resource
([native_lakehouse/main.tf](../infra/modules/native_lakehouse/main.tf)); every
other table is created by SQL DDL through `make ddl-aws`. See §12, deviation
A4.

## 4. AWS-native tables

| Layer | Table | Format | Partitioned by | Row key | One row is | Written by |
|---|---|---|---|---|---|---|
| Bronze | `bronze_trades_stream` | Parquet + projection | `ingest_date` | none, append-only | one trade as it arrived off the WebSocket | Firehose |
| Bronze | `bronze_perp_context` | JSON + projection | `ingest_date` | none, append-only | one perpetual-futures poll for one instrument | `perp_handler` Lambda |
| Bronze | `bronze_macro_observations` | JSON + projection | `ingest_date` | none, append-only | one macro series at one vintage | `macro_handler` Lambda |
| Silver | `silver_trades` | Iceberg | `(instrument_id, day(event_ts))` | `(venue, trade_id)` | one distinct valid trade | `merge_silver_trades.sql` |
| Silver | `silver_trades_quarantine` | Iceberg | `(ingest_date)` | `row_key`, a hash of the raw tuple | one distinct invalid trade, with the reason | `merge_silver_quarantine.sql` |
| Silver | `silver_perp_context` | Iceberg | `(instrument_id, day(snapshot_ts))` | `(instrument_id, snapshot_ts_us)` | one instrument on the 5-minute grid | `merge_silver_perp_context.sql` |
| Silver | `silver_macro` | Iceberg | `(series_id)` | `(series_id, observation_date, vintage_date)` | one observation as first published or as later revised | `merge_silver_macro.sql` |
| Gold | `gold_bars_1m` | Iceberg | `(instrument_id, day(window_end_ts))` | `(instrument_id, window_end_ts)` | one instrument-minute bar | `merge_gold_bars_1m.sql` |
| Ops | `backfill_manifest` | Iceberg | `(tier)` | `archive_key` | one archive file, with its load state and row count | `merge_manifest_outcomes.sql` |
| Staging | `archive_staging_klines` | CSV | none | none | one archive kline row, read once then deleted | `awsnative.backfill.staging` |
| Staging | `archive_staging_trades` | CSV | none | none | one archive trade row, read once then deleted | `awsnative.backfill.staging` |
| Ops | `backfill_outcomes` | JSON | none | none | one loader result | `awsnative.backfill.loader` |

DDL lives in [`awsnative/sql/ddl/`](../awsnative/sql/ddl/), one file per table,
numbered in creation order. `make ddl-aws` runs all of them and is idempotent.

### Why the Silver merges have no UPDATE branch

Three of the four Silver merges are `WHEN NOT MATCHED THEN INSERT` only, with
no update. A trade, a funding reading, and a published macro value are all
immutable observations. Re-reading the same source must not change a stored
row, and the 5-minute Bronze window overlaps between runs, so the merge sees
the same input many times. Only `gold_bars_1m` updates, because a minute's bar
changes when late trades land in that minute.

`silver_macro` stores a revision as a new row, keyed on `vintage_date`. A
revised CPI value does not overwrite the value you could have read at the time.

### The enrichment collectors

| Collector | Source | Lands in |
|---|---|---|
| `perp_handler` | Binance REST, 41 sequential requests | `bronze_perp_context` |
| `macro_handler` | ALFRED CSV, one request per series | `bronze_macro_observations` |

`silver_perp_context` carries the funding **quote** plus open interest, the
top-trader long/short ratios, the global account ratios, and the taker
buy/sell split. `silver_macro` carries six series: `DTWEXBGS`, `DGS2`,
`DGS10`, `VIXCLS`, `SP500`, and `CPIAUCSL`. Only `CPIAUCSL` gets revised, which
is why the vintage key exists.

Both collectors ship as one Lambda package built from the repo root and write
to S3 directly, with no Firehose and no stream: at 288 polls a day and about
4 KB each, buffering would add latency without saving a meaningful request
count. Neither uses a secret; the module header records why each is absent
rather than forgotten. Schedule, live status, and the merge gap: §6.

## 5. Data lineage

One row per pipeline. Every hop after "landing" is an Athena `MERGE INTO`
statement, not application code; the only writers of application code are
Firehose (trades) and the two Lambdas (enrichment).

| Pipeline | Source → landing → transform → table |
|---|---|
| Trades | Binance/Coinbase WS → `ingest/` on Fargate → Kinesis → Firehose → `bronze_trades_stream` → `merge_silver_trades.sql` → `silver_trades`, and `merge_silver_quarantine.sql` → `silver_trades_quarantine` → `merge_gold_bars_1m.sql` → `gold_bars_1m` |
| Perpetual context | Binance REST → `perp_handler` Lambda (direct `s3:PutObject`) → `bronze_perp_context` → `merge_silver_perp_context.sql`, its own state machine (§6) → `silver_perp_context` |
| Macro | ALFRED CSV → `macro_handler` Lambda (direct `s3:PutObject`) → `bronze_macro_observations` → `merge_silver_macro.sql`, its own state machine (§6) → `silver_macro` |
| Backfill (stage N4, unbuilt) | `data.binance.vision` archive → `awsnative.backfill.loader` → `archive_staging_klines` / `archive_staging_trades` → `merge_silver_from_archive.sql` / `merge_gold_from_archive.sql` → `silver_trades` / `gold_bars_1m`; load outcomes → `merge_manifest_outcomes.sql` → `backfill_manifest` |
| Dashboard | `silver_perp_context`, `silver_macro`, `gold_bars_1m` → `awsnative.dashboard` → `dashboard.html` (reads only, writes no table) |

## 6. Scheduling

Four independent trigger mechanisms feed this lake. None of them share a
scheduler, and arming one does not arm another.

| Writes | Trigger | Interval | Armed by | Shipped default |
|---|---|---|---|---|
| `bronze_trades_stream` | Firehose buffer flush | 128 MB or 120 s, whichever comes first | always, whenever the Fargate producer runs | n/a |
| `silver_trades`, `silver_trades_quarantine`, `gold_bars_1m` | EventBridge Scheduler → Step Functions `fdai-native-microbatch` | `rate(5 minutes)` | `microbatch_enabled` | `false` on first apply |
| `bronze_perp_context` | EventBridge Scheduler → `perp_handler` Lambda, direct invoke | `rate(5 minutes)` | `enrichment_enabled` | `false` |
| `bronze_macro_observations` | EventBridge Scheduler → `macro_handler` Lambda, direct invoke | `cron(30 6 * * ? *)` UTC | `enrichment_enabled` | `false` |
| `silver_perp_context` | EventBridge Scheduler → Step Functions `fdai-native-enrichment-merge-perp` | `rate(5 minutes)` | `enrichment_enabled` | `false` |
| `silver_macro` | EventBridge Scheduler → Step Functions `fdai-native-enrichment-merge-macro` | `cron(30 6 * * ? *)` UTC | `enrichment_enabled` | `false` |

Each merge state machine is a single `athena:startQueryExecution.sync` Task
behind the same overlap guard the trade micro-batch uses: a
`ListExecutions`/`Choice` pair that skips this tick if the previous run is
still going, since two concurrent `MERGE`s into the same Iceberg table pay
twice and then fail on the commit lock (§12, deviation A5). Both merges are
deliberately **separate** from `fdai-native-microbatch` and from each other:
folding the macro merge into a 5-minute cadence would rescan
`bronze_macro_observations` and `silver_macro` in full for data that changes
at most once a day, and a shared state machine would make `microbatch_enabled`
and `enrichment_enabled` stop being independent switches.

`terraform.tfvars` is gitignored, so the table above shows shipped defaults,
not what is armed in any one account. Check the live state:

```bash
terraform -chdir=infra/envs/native output microbatch_schedule
aws scheduler get-schedule --name fdai-native-microbatch --query State --output text
aws scheduler get-schedule --name fdai-native-enrichment-merge-perp --query State --output text
aws scheduler get-schedule --name fdai-native-enrichment-merge-macro --query State --output text
```

Run any stage once, regardless of what is armed:

```bash
make microbatch-aws    # one Silver+Gold cycle now; blocks until it finishes
make enrich-aws        # both collectors, once, synchronous
make merge-enrich-aws  # both Silver merges, once each; blocks until both finish
```

## 7. Freshness

Lag from the source event to a queryable row. Worst case unless noted.

| Table | Lag | Basis |
|---|---|---|
| `bronze_trades_stream` | up to 120 s | Firehose buffer interval; the 128 MB size threshold rarely fires first at current volume |
| `silver_trades`, `silver_trades_quarantine` | p50 ≈ 4 min | design spec §5.1; target SLO is p50 < 6 min producer-to-Silver |
| `gold_bars_1m` | same order as Silver, plus one more sequential Athena statement | Gold runs after both Silver branches complete, in the same execution |
| `bronze_perp_context` | up to 5 min | poll cadence |
| `bronze_macro_observations` | up to 24 h | one pull a day, after US markets settle |
| `silver_perp_context` | up to ~10 min | its merge runs on the same 5-minute schedule as the poll; a miss converges on the next tick, same tolerance as trades |
| `silver_macro` | up to 24 h | its merge runs right after the daily poll on the same schedule |

`awsnative/sql/dashboard/02_freshness.sql` computes the first four rows live,
as the lag between the newest row and now.

## 8. Estimated size

Design-time estimates from the specs, not measurements. Nothing in this stack
has run long enough in a real account to measure it (§13).

| Table | Basis | Estimate |
|---|---|---|
| `bronze_trades_stream` | ~300 msg/s steady, ~350 bytes/record JSON | ~26M records/day, ~9 GB/day before Parquet conversion; bursts 5 to 10x during volatility |
| `silver_trades` | same event count, minus duplicates | same order of magnitude; smaller at rest once Parquet and Iceberg encode it |
| `silver_trades_quarantine` | target under 0.1% of trade volume | up to ~26K rows/day at the SLO ceiling; a healthy run stays far below that |
| `gold_bars_1m` | 8 instruments × 1,440 minutes/day | 11,520 rows/day, well under 1 MB/day |
| `bronze_perp_context` | 288 polls/day × ~4 KB | ~1.2 MB/day |
| `bronze_macro_observations` | 1 pull/day, 6 series | well under 1 MB/day |
| `silver_perp_context` | 8 instruments × 288 samples/day | 2,304 rows/day |
| `silver_macro` | 6 series, 1 revised monthly | a handful of new rows/day |
| `backfill_manifest`, `archive_staging_klines`, `archive_staging_trades`, `backfill_outcomes` | stage N4 is unbuilt | 0 today |

`awsnative/sql/dashboard/01_layer_counts.sql` gives live row counts once data
exists. None of the above is a cost estimate; see
[`ARCHITECTURE.md`](ARCHITECTURE.md) §4.4 for the cost model.

## 9. Databricks tables

Catalog `fdai`, schema `market`. Defined in
[`lakehouse/pipelines/trades.py`](../lakehouse/pipelines/trades.py) and deployed
by [`resources/trades.pipeline.yml`](../resources/trades.pipeline.yml).

| Layer | Table | Format | Row key | One row is |
|---|---|---|---|---|
| Bronze | `bronze_trades_stream` | Delta, streaming table | none, append-only | one `md.trades.v1` record: decoded Avro, Kafka metadata, and the original bytes |
| Silver | `silver_trades` | Delta, AUTO CDC SCD Type 1 | `(venue, trade_id)` | one deduplicated trade fact |
| Silver | `silver_trades_quarantine` | Delta, streaming table | none | one rejected record, with the reason and the raw bytes |

Two temporary views sit between them: `trades_validated` (applies the
expectations) and `trades_clean` (the passing subset). They hold no storage.

Three pipeline settings are load-bearing, and each records its reason in
`trades.pipeline.yml`:

- `edition: ADVANCED`, because AUTO CDC and SCD Type 1 need it
- `serverless: false`, because MSK admits only the workspace NAT Elastic IP
- `continuous: false`, because nothing downstream of Silver consumes it yet

Gold does not exist on this side. The pipeline triggers on demand
(`make pipeline-run`); nothing schedules it on a cadence.

## 10. Column contracts

**Event time, never arrival time.** Bars key on `event_ts`, the exchange
timestamp. `ingest_ts` records arrival and never drives a window. A slow
consumer must not reshape the series. Both timestamps also appear as
microsecond integers (`event_ts_us`, `ingest_ts_us`) carried unchanged from the
wire format; compare on those when precision matters.

**Gold stores additive measures only.** `gold_bars_1m` holds `notional`,
`volume`, `buy_vol`, `sell_vol`, and `sq_log_return`, and leaves the reader to
divide. It does not store `vwap`, `flow_imbalance`, or `realized_vol`. A stored
ratio is correct at the grain it was computed at and wrong at every other, so
rolling 1-minute bars up to 5 minutes would silently produce a wrong VWAP.
[`awsnative/bars.py`](../awsnative/bars.py) states this contract as executable
Python so a test can check the DDL against it.

**OHLC is the one non-additive exception.** `open` and `close` are first and
last by event time, and no arithmetic recovers them from finer bars. Read paths
expose OHLC at 1-minute grain only.

**`source_tier` records where a row came from.** Stream-derived rows carry
`DERIVED_FROM_TRADES`. Archive-derived rows carry their archive tier, `DEEP`
(monthly klines) or `HOT` (daily aggTrades). The Gold merge refuses to
downgrade a stream-derived row to an archive-derived one. When a feature table
arrives, its `fidelity` column derives from this column.

**`knowledge_ts_us` and `vintage_date` are declared but not yet enforced.**
Both enrichment tables stamp when a fact became knowable. No read path filters
on them yet. Stage N5 adds the `*_pit` prepared statements, scopes Gold to the
tool server as the only IAM principal that can read it, and adds a
lookahead-injection test that must fail when the filter is removed. Until then,
treat these columns as recorded rather than guaranteed.

**Quarantine never drops a row.** The validity contract is one boolean
expression in
[`awsnative/sql/fragments/valid_trade.sql`](../awsnative/sql/fragments/valid_trade.sql).
Silver and quarantine use complementary predicates, both `COALESCE`-guarded,
because a `NULL` fails a predicate and its negation alike and a malformed row
would otherwise land in neither table.

## 11. What the layer answers today

Two acceptance suites and one dashboard read these tables.

| File | Answers |
|---|---|
| [`awsnative/sql/verify_bronze.sql`](../awsnative/sql/verify_bronze.sql) | Is Bronze receiving trades, and from which venues |
| [`awsnative/sql/verify_silver_gold.sql`](../awsnative/sql/verify_silver_gold.sql) | Seven acceptance queries over Silver and Gold, each reporting bytes scanned |
| `awsnative/sql/dashboard/01_layer_counts.sql` | Row counts per layer |
| `awsnative/sql/dashboard/02_freshness.sql` | Lag between the newest row and now |
| `awsnative/sql/dashboard/03_quarantine.sql` | Quarantine rate, by reason |
| `awsnative/sql/dashboard/04_perp_context.sql` | Funding, open interest, and positioning |
| `awsnative/sql/dashboard/05_macro.sql` | Macro series by vintage |

Run them with `make verify-aws` and `make dashboard-aws`. The dashboard answers
two questions: is the pipeline running, and what does the enrichment show that
price and size alone cannot.

## 12. Where the implementation differs from the design

The design is
[`superpowers/specs/2026-08-14-aws-native-workstream-design.md`](superpowers/specs/2026-08-14-aws-native-workstream-design.md);
§14 records these in full.

| # | Design said | Implementation does | Why |
|---|---|---|---|
| A1 | Quarantine is an `INSERT` (§5.4) | a `MERGE` keyed on a row hash | The Bronze window overlaps between runs, so an `INSERT` re-adds the same bad row 288 times a day |
| A2 | Two complementary predicates (§5.4) | the same, `COALESCE`-guarded on both sides | A `NULL` fails a predicate and its negation, so a malformed row would land in neither table |
| A3 | Dirty partition set passed between states (D3) | derived as a CTE from the same Bronze window | `MERGE` reports no affected rows. D3's real invariant, one shared bars computation, survives as a rendered SQL fragment |
| A4 | `terraform apply` reproduces a stage (§8) | `make up-aws` does; `terraform apply` alone does not create the Iceberg tables | Glue's `CreateTable` cannot express `day(event_ts)` under any configuration |
| A5 | not specified | the state machine skips when an execution is already running | Concurrent Iceberg merges pay twice and then fail on the commit lock |
| A6 | Bronze partitioned daily (§5.1) | unchanged, after measuring the alternative | Hourly partitions look right until you measure them against §10's ~130 hours a month; then they save nothing and cost a rewrite of live N1 infrastructure |
| n/a | Perp poller writes through Firehose ([enrichment design](superpowers/specs/2026-08-17-data-layer-enrichment-derivatives-and-macro-design.md) §6) | writes directly with `s3:PutObject`, no Firehose | At 288 polls/day and ~4 KB each, buffering adds latency and no meaningful saving; see the `native_enrichment` module header |
| n/a | Perp/macro merges run inside the trade micro-batch (same design, §6) | each gets its own state machine and schedule, in `native_enrichment` | Folding macro into the 5-minute trade cadence would rescan two tables in full 288 times a day; separate schedules also keep `microbatch_enabled` and `enrichment_enabled` independent, as designed |

## 13. What the layer does not do yet

- **No history.** Silver and Gold hold only what has streamed in since the last
  `make up-aws`. Stage N4's backfill is what makes the weekly wipe survivable.
- **No reconciliation.** Comparing stream-derived bars against archive klines
  needs the archive loaded first, so it belongs to N4. Running it now would
  produce a check that cannot fail.
- **No cross-venue price check.** Coinbase and Binance both feed Silver, but no
  windowed comparison against a cross-venue median exists, and no quarantine
  reason covers it.
- **No point-in-time boundary.** Anything with Athena access can read Gold. See
  §10 on `knowledge_ts_us`.
- **No table maintenance.** Nothing runs `OPTIMIZE` or `VACUUM`. The
  insert-only tables accumulate small files and `gold_bars_1m` accumulates
  merge-on-read delete files on every micro-batch. The symptom shows up first
  as rising query time.
- **Nothing here has run in an account.** Every number in the repo comes from a
  real API response or a real archive file, but no deployment has happened. The
  assumptions in each design's X-table stay open until someone runs
  `make up-aws`.
