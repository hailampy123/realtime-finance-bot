# Data layer

Every table this project stores, in both workstreams. For each one: where the
bytes live, how the table is partitioned, what a single row means, and which
process writes it.

Read this when you need to query the data or add a table. For the running
processes that fill these tables, see [`ARCHITECTURE.md`](ARCHITECTURE.md). For
what to build next, see
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

| Collector | Schedule (default) | Source | Lands in |
|---|---|---|---|
| `perp_handler` | `rate(5 minutes)` | Binance REST, 41 sequential requests | `bronze_perp_context` |
| `macro_handler` | `cron(30 6 * * ? *)` | ALFRED CSV, one request per series | `bronze_macro_observations` |

`silver_perp_context` carries the funding **quote** plus open interest, the
top-trader long/short ratios, the global account ratios, and the taker
buy/sell split. `silver_macro` carries six series: `DTWEXBGS`, `DGS2`,
`DGS10`, `VIXCLS`, `SP500`, and `CPIAUCSL`. Only `CPIAUCSL` gets revised, which
is why the vintage key exists.

Both collectors ship as one Lambda package built from the repo root. Neither
uses a stream or a secret; the module header records why each is absent rather
than forgotten.

## 5. Databricks tables

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

Gold does not exist on this side.

## 6. Column contracts

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

## 7. What the layer answers today

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

## 8. Where the implementation differs from the design

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

## 9. What the layer does not do yet

- **No history.** Silver and Gold hold only what has streamed in since the last
  `make up-aws`. Stage N4's backfill is what makes the weekly wipe survivable.
- **No reconciliation.** Comparing stream-derived bars against archive klines
  needs the archive loaded first, so it belongs to N4. Running it now would
  produce a check that cannot fail.
- **No cross-venue price check.** Coinbase and Binance both feed Silver, but no
  windowed comparison against a cross-venue median exists, and no quarantine
  reason covers it.
- **No point-in-time boundary.** Anything with Athena access can read Gold. See
  §6 on `knowledge_ts_us`.
- **No table maintenance.** Nothing runs `OPTIMIZE` or `VACUUM`. The
  insert-only tables accumulate small files and `gold_bars_1m` accumulates
  merge-on-read delete files on every micro-batch. The symptom shows up first
  as rising query time.
- **Nothing here has run in an account.** Every number in the repo comes from a
  real API response or a real archive file, but no deployment has happened. The
  assumptions in each design's X-table stay open until someone runs
  `make up-aws`.
