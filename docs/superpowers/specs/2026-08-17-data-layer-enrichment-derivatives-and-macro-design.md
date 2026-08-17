# Data Layer Enrichment: Derivatives Context and Macro Regime — Design

**Date:** 2026-08-17
**Status:** proposal, no implementation
**Slices:** E1 (derivatives context) and E3 (macro regime), designed together
**Parent specs:** [`2026-08-14-aws-native-workstream-design.md`](2026-08-14-aws-native-workstream-design.md),
[`2026-08-07-finance-data-ai-platform-design.md`](2026-08-07-finance-data-ai-platform-design.md),
[`2026-08-08-data-layer-batch-history-and-serving-design.md`](2026-08-08-data-layer-batch-history-and-serving-design.md)

Two of four enrichment slices. E2 (liquidity and execution realism) and E4 (narrative and
news) are out of scope and named in §14.

---

## 1. Purpose

Today Gold holds price, size, side and their aggregates. That is what happened at the tape.
A trading decision needs two things the tape cannot state:

**Positioning.** Is the market crowded, and on which side? "Price rose" means one thing when
open interest rose with it, and the opposite when open interest fell. The first says new
money entered. The second says shorts covered and the move has spent itself. The current
layer cannot tell these apart.

**Regime.** Is this a crypto signal or a beta signal? A long call that only works when the
dollar falls is a macro trade wearing a crypto label. Without a macro series the agent cannot
know which one it made, and neither can the backtest.

Both are **informational** in this slice. The risk engine is unchanged. A veto threshold that
nobody has backtested is a rule nobody can defend, and §7.5's "the LLM proposes; code
disposes" stays exactly as it is.

## 2. Scope

| Layer | In scope | Deliberately out |
|---|---|---|
| Sources | Binance USD-M perpetual context, FRED macro | on-chain, social, ETF flows, CME basis |
| Ingest | one REST poller, one daily macro pull | any change to `ingest/`, any new WebSocket stream |
| Bronze | `bronze_perp_context`, `bronze_macro_observations` | — |
| Silver | `silver_perp_funding`, `silver_perp_positioning`, `silver_macro` | — |
| Gold | none; the join happens at read time (§7.3) | `gold_context_1m` (rejected, §13) |
| Serving | two new narrow tools | any change to the risk engine |
| Backfill | `fundingRate` and `metrics` archive tiers | `bookDepth` (that is E2) |

## 3. The sources, verified

Every number in this section comes from a live request or a downloaded file, not from
documentation.

### 3.1 Binance USD-M perpetuals

**The archive path.** Both types live in `data.binance.vision`, the bucket §6.2's loader
already reads, with a `.CHECKSUM` sibling per file.

| Path | Columns | Size | Grain |
|---|---|---|---|
| `data/futures/um/monthly/fundingRate/{SYM}/` | `calc_time, funding_interval_hours, last_funding_rate` | **914 B** per month per symbol | 8 h |
| `data/futures/um/daily/metrics/{SYM}/` | `create_time, symbol, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio` | **11 KB** per day per symbol | 5 min |

**The live path.** Four REST endpoint families, all public and unauthenticated.

| Endpoint | Covers | Calls per poll |
|---|---|---|
| `/fapi/v1/premiumIndex` (no `symbol`) | `markPrice`, `indexPrice`, `estimatedSettlePrice`, `lastFundingRate`, `interestRate`, `nextFundingTime` for **874 symbols in one 194 KB response** | 1 |
| `/fapi/v1/openInterest?symbol=` | `openInterest` | 8 |
| `/futures/data/topLongShortAccountRatio`, `topLongShortPositionRatio`, `globalLongShortAccountRatio`, `takerlongshortRatio` (`period=5m`) | the four ratios | 32 |

**41 requests per poll against a limit of 1000 per 5 minutes per IP: 4.1% of budget.**

**Every archive column has a live counterpart**, which is what makes the convergence check of
§6.4 a real comparison rather than a proxy. The reverse does not hold, and §4.2 makes use of
that:

| Archive column | Live source | Live also gives |
|---|---|---|
| `last_funding_rate` | `premiumIndex.lastFundingRate` | `interestRate`, `estimatedSettlePrice` |
| `sum_open_interest` | `openInterest.openInterest` | — |
| `sum_open_interest_value` | `openInterest × premiumIndex.markPrice` | — |
| `count_toptrader_long_short_ratio` | `topLongShortAccountRatio.longShortRatio` | **`longAccount`, `shortAccount`** |
| `sum_toptrader_long_short_ratio` | `topLongShortPositionRatio.longShortRatio` | **`longAccount`, `shortAccount`** |
| `count_long_short_ratio` | `globalLongShortAccountRatio.longShortRatio` | **`longAccount`, `shortAccount`** |
| `sum_taker_long_short_vol_ratio` | `takerlongshortRatio.buySellRatio` | **`buyVol`, `sellVol`** |

**The live path is strictly richer than the archive, and that asymmetry is load-bearing.**
For all four ratios the live endpoints return the components the ratio was built from. The
archive returns the ratio alone. §4.2 and §7.2 are the consequences.

**The REST history is not a substitute for the archive.** `/futures/data/openInterestHist`
retains the latest **1 month** only. The daily `metrics` archive covers years. Backfill and
live are both required, not alternatives.

### 3.2 FRED macro

Six series, five daily and one monthly, through the free FRED API.

| Series | What | Frequency | Revised? |
|---|---|---|---|
| `DTWEXBGS` | Nominal broad U.S. dollar index | daily | no |
| `DGS2` | 2-year Treasury yield | daily | no |
| `DGS10` | 10-year Treasury yield | daily | no |
| `VIXCLS` | CBOE VIX | daily | no |
| `SP500` | S&P 500 index (10 years of history) | daily | no |
| `CPIAUCSL` | CPI, all urban consumers | monthly | **yes** |

The 2s10s spread is **not** a series in this set. `DGS10 - DGS2` is derivable, and §5.3's
rule says store the components and let the reader subtract. The same rule that forbids a
stored `vwap` forbids a stored spread.

`CPIAUCSL` is in the set for one reason: it is the series that makes the vintage machinery of
§5 load-bearing rather than decorative. The five market series are published once and never
revised, so they would exercise nothing. **If you cut `CPIAUCSL`, cut the vintage handling
with it**, and record that as a named cut.

**FRED requires a free API key.** §7.3 of the parent design states there is "no API key
anywhere at all." That claim narrows to **no LLM API key**. The FRED key is read-only access
to public government data with no financial exposure, and it goes in Secrets Manager. Saying
so is the alternative to quietly adding a secret to a design that advertises having none.

## 4. Two traps, named before they bite

The repo already carries two named parsing traps (§6.3: `isBuyerMaker` inverts, timestamp
units change mid-range). These are the equivalents for this data, and both get golden-file
tests.

### 4.1 `count_` and `sum_` are weighting schemes, not aggregations

In the `metrics` archive the prefixes look like SQL aggregate functions and are not:

- `sum_open_interest` **is** a sum.
- `count_toptrader_long_short_ratio` is the top-trader long/short ratio weighted **by
  account**, where every account counts once.
- `sum_toptrader_long_short_ratio` is the same ratio weighted **by position size**, so it is
  dollar-weighted.
- `count_long_short_ratio` is the global account-weighted ratio. The archive has **no**
  global position-weighted twin.
- `sum_taker_long_short_vol_ratio` is a taker buy/sell volume ratio.

The divergence between the account-weighted and the position-weighted top-trader ratio is
itself a signal: many small accounts long while large positions sit short is a different
market from the reverse. Swap the two columns and the feature inverts, the data stays
well-formed, and no downstream check fires. That is the same failure shape as
`isBuyerMaker`.

### 4.2 The archive form of every ratio is not additive; the live form is

`count_long_short_ratio` at 5-minute grain cannot be averaged to get the hourly value, for
exactly the reason §5.3 forbids a stored `vwap`: an average of ratios is not the ratio of the
aggregates.

For **archive** rows the components cannot be recovered, because the file stores only the
ratio. For **live** rows they can: the endpoints return `longAccount` and `shortAccount`, or
`buyVol` and `sellVol` (§3.1). So §5.3's rule applies to the live path in full and cannot
apply to the archive path at all.

Three consequences, all stated rather than worked around:

1. **Store the components whenever they exist.** `silver_perp_positioning` carries both the
   components and the ratio. Live rows fill both. Archive rows fill the ratio and leave the
   components `NULL`. §7.2 states the schema.
2. **A rollup is valid only over live rows.** Any query that aggregates a ratio across time
   must filter `source_tier = 'LIVE_POLL'` and divide summed components. Over archive rows it
   must return the native-grain value or nothing. A rollup that silently mixes the two is the
   same class of quiet error as a stored `vwap`.
3. **None of them enters `gold_bars_1m`.** That table's contract is additive components only,
   asserted offline by [test_sql_contracts.py](tests/awsnative/test_sql_contracts.py) against
   [bars.py](awsnative/bars.py). Mixed-nullability components would not satisfy it. §7.3 is
   the consequence.

This is the same fidelity split §6.4 already handles for bars, in a new place: two tiers of
the same measure, one richer than the other, and a marker that says which you are reading.
`source_tier` is that marker here too.

Open interest is exempt from the ratio problem and fails a different way: it is a **level**,
not a flow. Summing open interest across five minutes is meaningless. Its signal is the
first difference, which the reader computes.

## 5. The `knowledge_ts` invariant

**Every row in every new table carries `knowledge_ts_us`: the earliest instant at which this
value could have been known.** It is separate from `event_ts_us`, and read paths filter on
`knowledge_ts`, never on `event_ts`.

| Source | `knowledge_ts` is | Why it differs from `event_ts` |
|---|---|---|
| funding | the settlement instant | the rate is published at settlement, so the two coincide |
| positioning | the poll instant | a 5-minute snapshot stamped `create_time` is observable only after the interval closes |
| macro, market series | the release instant | a daily close is knowable that evening, not at 00:00 that day |
| macro, `CPIAUCSL` | the ALFRED **`vintage_date`** | the observation is stamped with the month it measures and is published six weeks later, then revised |

The macro row is the one that matters. A CPI observation for January carries
`observation_date = 2026-01-01`. Its first print lands in mid-February and gets revised
later. Join on `observation_date <= as_of` and a backtest reads a number nobody had, plus a
revision nobody had. Join on `vintage_date <= as_of` and it reads what the market read.

Silver already carries `event_ts` alongside `ingest_ts` (§5.2), so this is that idea made
explicit and given a name, not a new concept.

`knowledge_ts` is what makes the third correctness property of §1 hold for a data class
whose publication lags its measurement. Without it, macro enrichment is a lookahead leak
with a nice chart.

## 6. Architecture

### 6.1 The simplification: no new stream, and `ingest/` is untouched

An earlier shape for this slice used a Binance futures WebSocket connector for funding plus a
poller for positioning. Two facts removed the WebSocket half:

1. **Funding changes every 8 hours.** The `markPrice@1s` stream would deliver the same value
   28,800 times between changes. A 5-minute poll captures it with 96× more resolution than
   the data carries.
2. **Open interest and the four ratios have no native stream.** They are REST-only. Any live
   path therefore contains a poller regardless, so the WebSocket adds a second component
   class for nothing.

Dropping it removes a new connector, a second Kinesis stream and its per-stream-hour charge,
and one further consequence worth stating on its own.

`ingest/core/sinks.py` types the seam as `produce(topic: str, trade: Trade)`, and
`connectors/base.py` types `parse` as returning `list[Trade]`. A funding rate is not a
`Trade`. Pushing observations through that seam would mean generalising a protocol that
`runner.py`, `producer.py` and `KinesisSink` all depend on, which contradicts §4.1's claim
that "no refactor of tested code is required." **The poller lives in `awsnative/`, where boto3
already lives, and `ingest/` does not change.** Same reasoning that put `KinesisSink` outside
`ingest/` in the first place.

### 6.2 Topology

```text
                                       ┌── 5 min ──────────────────────────────┐
fapi.binance.com  ── 41 requests ──>   │ Lambda: perp context poller           │
                                       │ awsnative/pollers/perp_context.py     │
                                       └──── PutRecordBatch ───────────────────┘
                                                     │
                                       Firehose (Direct PUT, JSON->Parquet)
                                                     │
                                                     v
                                          bronze_perp_context
                                                     │
                                       ┌── the EXISTING micro-batch, 2 new states
                                       v
                            silver_perp_funding · silver_perp_positioning

api.stlouisfed.org ── 6 series ──>  ┌── daily ────────────────────┐
                                    │ Lambda: macro pull          │──> S3 Parquet
                                    │ awsnative/pollers/macro.py  │        │
                                    └─────────────────────────────┘        v
                                                              bronze_macro_observations
                                                                          │
                                                                          v
                                                                   silver_macro

data.binance.vision ── fundingRate + metrics ──> the EXISTING N4 backfill loader
                                                  ──> the same three Silver tables
```

Two ingestion shapes, because two data volumes:

- **Perp context** flows through Firehose with Direct PUT. No Kinesis stream, so no
  per-stream-hour charge, and Firehose's buffer solves the small-file problem the poller
  would otherwise create. Buffer 300 s. Result: ~288 files a day, about 4 KB each, ~1.2 MB a
  day in total.
- **Macro** writes one small Parquet file a day straight to S3. Six series at daily frequency
  is one file; a Firehose in front of it would be a component with no job.

### 6.3 New components

| Component | Its one job | Where | Survives the wipe? |
|---|---|---|---|
| perp context poller | 41 requests, one JSON batch to Firehose | Lambda, `native_enrichment` | no |
| macro pull | 6 FRED series with vintages, one Parquet file | Lambda, `native_enrichment` | no |
| Firehose delivery stream #2 | land `bronze_perp_context` as Parquet | `native_enrichment` | no |
| 2 EventBridge schedules | 5 min and daily | `native_enrichment` | no |
| Secrets Manager secret | the FRED API key | `native_enrichment` | no |
| 2 new micro-batch states | merge Bronze into the three Silver tables | `native_medallion` | no |

## 7. Tables

### 7.1 Bronze

Both are plain Parquet with partition projection, following D1 and §5.1. Append-only, so
Iceberg buys nothing.

- `bronze_perp_context`, partitioned by `ingest_date`. **One wide row per instrument per
  poll**, carrying every raw field from all four endpoint families, and carrying each family's
  own source timestamp as its own column (`premium_index_ts`, `open_interest_ts`,
  `top_account_ratio_ts`, and so on) alongside `poll_ts_us`. The four ratio endpoints return
  their own 5-minute grid timestamps, which need not equal the poll instant, and flattening
  them to one timestamp would destroy the evidence X1 exists to check.
- `bronze_macro_observations`, partitioned by `ingest_date`, one row per
  `(series_id, observation_date, vintage_date)`.

Raw strings for every decimal, cast in Silver. Same discipline as `bronze_trades_stream`,
where `price` and `size` are strings until Silver casts them.

### 7.2 Silver

Three Iceberg tables, each at its source's native grain. Nothing is resampled, and nothing
is forward-filled.

| Table | Key | Partition | Grain | Rows per day, 8 instruments |
|---|---|---|---|---|
| `silver_perp_funding` | `(instrument_id, funding_ts_us)` | `(instrument_id, day(funding_ts))` | 8 h | 24 |
| `silver_perp_positioning` | `(instrument_id, snapshot_ts_us)` | `(instrument_id, day(snapshot_ts))` | 5 min | 2,304 |
| `silver_macro` | `(series_id, observation_date, vintage_date)` | `(series_id)` | daily and monthly | ~6 |

Every table carries `event_ts_us`, `knowledge_ts_us`, `source` and `source_tier`.
`source_tier ∈ {LIVE_POLL, ARCHIVE}`, mirroring `gold_bars_1m`'s existing fidelity marker so
the same homogeneity assertion of §6.4 applies.

`silver_perp_positioning` carries **both the components and the ratio** for each of the four
ratios, per §4.2:

| Column group | Live rows | Archive rows |
|---|---|---|
| `toptrader_long_accounts`, `toptrader_short_accounts` | filled | `NULL` |
| `toptrader_ratio_accounts` | filled | filled |
| `toptrader_long_positions`, `toptrader_short_positions` | filled | `NULL` |
| `toptrader_ratio_positions` | filled | filled |
| `global_long_accounts`, `global_short_accounts` | filled | `NULL` |
| `global_ratio_accounts` | filled | filled |
| `taker_buy_vol`, `taker_sell_vol` | filled | `NULL` |
| `taker_buy_sell_ratio` | filled | filled |
| `open_interest`, `open_interest_value` | filled | filled |

A `NULL` component here means "the archive does not carry it", never "the value was zero". The
validity predicate of §5.4 must not quarantine an archive row for a `NULL` it is expected to
have, which is the `COALESCE` trap of §14 A2 in a new place.

`snapshot_ts_us` in `silver_perp_positioning` is the **5-minute grid point the observation
belongs to**, obtained by flooring the endpoint's own timestamp to the grid. Bronze keeps the
four raw timestamps (§7.1), so the flooring is auditable rather than lossy.

All three tables merge with `WHEN NOT MATCHED THEN INSERT` only, for §5.2's reason: a settled
funding rate and a closed 5-minute snapshot are immutable facts. `silver_macro` shows why that
choice generalises rather than needing an exception. A CPI revision is a **new**
`vintage_date`, so it arrives as a new key and inserts. The revision history is preserved by
construction, and an `UPDATE` branch would destroy exactly the history §5 depends on.

Consequence worth naming: all three tables are insert-only, so §14 A7 of the parent design
applies to them. They accumulate small data files and no delete files, and the maintenance
pass proposed in
[`2026-08-17-iceberg-table-maintenance-design.md`](2026-08-17-iceberg-table-maintenance-design.md)
must cover them.

### 7.3 There is no new Gold table, and that is the design

`gold_bars_1m` gains no columns. §4.2 gives the reason: funding rate and the four ratios are
ratios, open interest is a level, and the table's contract is additive components only.

A separate materialized `gold_context_1m` at 1-minute grain is also rejected (§13). It would
store 96 redundant copies of every 8-hour funding rate, rewrite 1,440 rows per series on
every macro revision, and put a lookahead bug in the forward-fill boundary.

The join happens at read time instead.

## 8. The as-of join, and where the boundary lives

§7.1 of the parent design puts the point-in-time boundary in two places: the `*_pit` prepared
statements, and IAM. The as-of join goes in the same prepared statements, so the boundary
does not grow a third location.

The shape, for one instrument at one instant:

```sql
PREPARE perp_context_pit FROM
WITH latest_funding AS (
    SELECT funding_rate, funding_ts, knowledge_ts
    FROM silver_perp_funding
    WHERE instrument_id = ? AND knowledge_ts <= ?
    ORDER BY knowledge_ts DESC LIMIT 1
),
latest_positioning AS (
    SELECT open_interest, open_interest_value,
           toptrader_ratio_accounts, toptrader_ratio_positions,
           global_ratio_accounts, taker_buy_sell_ratio,
           snapshot_ts, knowledge_ts
    FROM silver_perp_positioning
    WHERE instrument_id = ? AND knowledge_ts <= ?
    ORDER BY knowledge_ts DESC LIMIT 1
)
SELECT * FROM latest_funding, latest_positioning
```

Three properties this shape has:

- **The filter is on `knowledge_ts`, never `event_ts`.** §5 is the reason.
- **The returned row states its own age.** `knowledge_ts` comes back as a column, so a caller
  can see that the positioning reading is 40 minutes stale rather than assume it is current.
  A stale reading presented as fresh is the failure mode §6.4 warns about, in a new place.
- **The existing CI contract test extends to it for free.** That test parses every `*_pit`
  `.sql` file and asserts it declares an `as_of` parameter. These files do, so the test covers
  the new sources without a change.

The read cost is small by construction. `silver_perp_funding` holds 3 rows per instrument per
day and `silver_perp_positioning` holds 288. Two ordered lookups over partition-pruned tables
of that size are cheaper than the bar query they accompany.

A second statement, `macro_context_pit`, does the same over `silver_macro` with
`vintage_date <= ?` per series.

## 9. Tool surface

Two new narrow tools, following §7.2's principle that a narrow typed tool is easier for a
model to use correctly and easier to evaluate than a general one. Neither existing tool
changes shape.

| Tool | Returns | Backed by |
|---|---|---|
| `get_perp_context(instrument_id, as_of)` | funding rate, next funding time, open interest, the four ratios **and their components where present**, mark/index prices, `source_tier`, and the `knowledge_ts` of each group | `perp_context_pit` |
| `get_macro_context(as_of)` | the six series as known at `as_of`, each with its `vintage_date` | `macro_context_pit` |

The tool server still holds no aggregation logic (§7.2). It marshals parameters and injects
`as_of`.

`get_perp_context` returns levels, and the agent's prompt must say so, because the signal is
in the change. The prompt therefore also receives the reading from one hour before `as_of`,
obtained by calling the same tool twice. Two calls of an honest tool beat one call of a tool
that computes a delta and hides which two instants it used.

## 10. Testing

| Layer | What | Offline? |
|---|---|---|
| Unit | `metrics` parser vs a golden fixture: the four ratios land in the right columns, `count_` versus `sum_` not swapped | yes |
| Unit | `fundingRate` parser: `calc_time` is epoch **milliseconds**, unlike aggTrades (§6.3's unit trap in a new place) | yes |
| Unit | FRED vintage parser: two vintages of one `CPIAUCSL` observation produce two rows, not an update | yes |
| Unit | poller request budget: one poll issues exactly 41 requests | yes |
| Contract | every new `*_pit` `.sql` declares an `as_of` parameter | yes |
| Contract | no new column enters the `gold_bars_1m` DDL | yes |
| Contract | every new Silver DDL declares `knowledge_ts_us` | yes |
| Contract | every ratio column in `silver_perp_positioning` has a declared component pair, so §4.2's rule cannot be half-applied | yes |
| Unit | an archive row with `NULL` components passes the validity predicate and is **not** quarantined (§14 A2's trap in a new place) | yes |
| Lookahead | insert a `CPIAUCSL` vintage dated after `as_of`; assert `macro_context_pit` excludes it. **Must fail if the filter moves to `observation_date`.** | no, dev Athena |
| Integration | live poll and archive backfill of the same 5-minute window converge column for column | no, dev Athena |

The last row is the one that earns the design. The live and archive column sets correspond one
for one (§3.1), so the convergence check is a real comparison and not the vacuous green check
§6.4 forbids.

## 11. Cost

Added to §10 of the parent design.

| Item | Estimate at ~130 h/month |
|---|---|
| Lambda, perp poller: 288 invocations/day, ~2 s, 256 MB | under $0.10 |
| Lambda, macro pull: 1 invocation/day | negligible |
| Firehose Direct PUT, ~1.2 MB/day | under $0.05 |
| Athena, 2 extra merge statements per micro-batch over ~1.2 MB | inside §10's existing ~$4 line |
| S3 storage and requests | negligible |
| Secrets Manager, 1 secret | $0.40/month |
| **Total added** | **under $1/month** |

The parent design's `~$25–35/mo` total does not change. The cost of this slice is engineering
time, not AWS spend, which is the honest thing to compare.

No always-on compute is added. Both new components are scheduled Lambdas.

## 12. Assumptions to verify

| # | Assumption | Verify with | Fallback if false |
|---|---|---|---|
| X1 | `/futures/data/*` at `period=5m` returns rows aligned to the archive's `create_time` grid | one poll against one archive day | store both and reconcile on the nearest preceding grid point |
| X2 | `calc_time` in `fundingRate` is epoch milliseconds for the whole 2-year range | magnitude check across the range, as §6.3 does for aggTrades | per-file unit detection, the same fix §6.3 already uses |
| X3 | Firehose Direct PUT accepts the poller's batch and converts it against the Glue schema | one poll into a dev delivery stream | poller writes Parquet to S3 directly, as the macro pull does |
| X4 | FRED's `realtime_start`/`realtime_end` returns `CPIAUCSL` vintages on the free tier | one request for a known revised month | drop `CPIAUCSL` and the vintage handling with it (§3.2) |
| X5 | `estimatedSettlePrice` and `nextFundingTime` are non-zero for all 8 instruments | one `premiumIndex` call, filtered to the universe | treat zero as `NULL` and quarantine, per §5.4's rule that nothing is silently dropped |
| X6 | The 8 instruments' USDT perps have `metrics` archive coverage for the full backfill window | list the archive prefix per symbol | shorten the window per instrument and record coverage, per §6.4 |

X5 is real, not hypothetical: a `premiumIndex` call in this session returned
`"estimatedSettlePrice": "0.00000000"` and `"nextFundingTime": 0` for `BAKEUSDT`.

## 13. Rejected alternatives

- **Columns on `gold_bars_1m`.** Breaks the additive-components contract (§4.2) and would
  require weakening an existing offline test.
- **A materialized `gold_context_1m` at 1-minute grain.** Simple reads and an easier
  dashboard, at the cost of 96 redundant copies of each funding rate, a 1,440-row rewrite per
  series per macro revision, and a forward-fill boundary that is a silent lookahead bug.
  Retained as the named fallback if §8's as-of join measures badly.
- **A Binance futures WebSocket connector.** §6.1. The stream would repeat an 8-hourly value
  every second, and positioning needs a poller regardless.
- **A second Kinesis stream for observations.** Firehose Direct PUT reaches Bronze without
  one, and an on-demand stream carries a per-stream-hour charge for a few KB an hour.
- **Generalising `Sink`/`Connector` to carry non-trade records.** Contradicts §4.1's claim
  that tested code needs no refactor. The poller sits in `awsnative/` instead, for the same
  reason `KinesisSink` does.
- **A deterministic funding or crowding veto in the risk engine.** The threshold would be
  invented rather than measured. Revisit once the backtest can score it.
- **On-chain metrics (CryptoQuant, Glassnode).** Their history sits behind a paid tier, so
  the weekly re-derivation model cannot rebuild it. Rejected on availability under this
  project's durability model, not on merit.
- **Fear & Greed (alternative.me).** Free and trivial to pull, but 365 days of history, no
  checksum, and it is a composite of inputs this design already collects directly. Low value
  per component.
- **`bookTicker` and `bookDepth`.** Both exist in the archive and both belong to E2. A
  `bookTicker` daily request for 2026-08-15 returned HTTP 404 in this session, so E2 should
  verify daily coverage before planning on it.

## 14. What this does not cover

- **E2, liquidity and execution realism.** `bookDepth` is in the archive at 553 KB per day
  per symbol. It is what would let the risk engine constrain `size_pct` against real depth.
  Today that field is unconstrained by any liquidity fact, which is a genuine gap and a
  separate slice.
- **E4, narrative.** News and sentiment. `news.articles.v1` has never had a producer in
  either workstream. GDELT guarantees roughly 3 months of history and CryptoPanic's free tier
  is capped, so the re-derivation model is weakest exactly here.
- **`gold.features` as its own table.** Still out of scope per §2 of the parent design. When
  it arrives, its `fidelity` column derives from `source_tier`, and these tables now carry
  that marker too.
- **Any change to the risk engine or to `gold_decisions`.** Informational only, by decision.
- **Coin-margined perpetuals and options.** USD-M only.
- **Measured read cost for §8's as-of join.** Argued from row counts, not measured. X-series
  verification does not cover it; the integration test should report it.

---

## Sources

- [Binance public data archive](https://data.binance.vision/) (listings and files downloaded directly)
- [binance-public-data](https://github.com/binance/binance-public-data)
- [Binance mark price stream](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream)
- [Binance open interest statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics)
- [Binance top trader long/short account ratio](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Long-Short-Account-Ratio)
- [Binance top trader long/short position ratio](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio)
- [Binance funding rate history](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History)
- [FRED API](https://fred.stlouisfed.org/docs/api/fred/)
- [FRED real-time periods (ALFRED vintages)](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
- [FRED series vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html)
