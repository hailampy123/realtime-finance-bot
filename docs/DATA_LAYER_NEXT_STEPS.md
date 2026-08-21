# Data layer: what to extend next

**Date:** 2026-08-18
**Status:** draft, for discussion

Where the data layer stands after slices E1 and E3, what to add next, and what to
integrate it with. Items are ordered by value per unit of effort, and each one
states why it sits where it does.

What exists today, table by table: [`DATA_LAYER.md`](DATA_LAYER.md). What runs
it: [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## 1. Starting point

Twelve tables in the AWS-native lake and three on the Databricks side. The
dashboard reads them and answers two questions: is the pipeline running, and
what does the enrichment show that price and size alone cannot.

**Verified offline, never deployed.** Every number in this repo comes from a real
API response or a real archive file, but nothing here has run in an account. The
account is wiped weekly, so the assumptions in each design's X-table stay open
until someone runs `make up-aws`.

## 2. Extending the data

### 2.1 E2, liquidity: the gap that matters most

The agent's decision output carries a `size_pct`, and **nothing in the data layer
can say whether that size is executable.** `bookDepth` already sits in the
archive at 553 KB per day per symbol, with depth and notional at ±1% to ±5% from
mid.

Three things become possible with it:

- a slippage estimate for a proposed size, so a decision can be costed
- a hard cap the risk engine can enforce, which is the first enrichment that
  would justify moving from informational to enforcing
- a liquidity-adjusted view of the flow imbalance already in Gold

The cost is one more archive tier through a loader that already exists. This is
the highest value per unit of effort on the page.

### 2.2 Settled funding, from the archive

`silver_perp_context` holds the funding **quote**, sampled every five minutes.
The monthly `fundingRate` archive files hold what actually settled, with the
interval per symbol. The two answer different questions, and the gap between
quote and settlement is itself a signal. Loading them also resolves an ambiguity
the design recorded rather than guessed: whether `lastFundingRate` describes the
last settlement or the next one.

Small: 914 bytes per month per symbol, and the loader already reads this bucket.

### 2.3 Cross-venue price sanity

Deferred since the first design (§5.4). Coinbase and Binance both feed Silver, so
the data to compare is already there. What is missing is a windowed check against
the cross-venue median, and a quarantine reason for a price that fails it. It
closes the last named hole in the validity contract.

### 2.4 E4, narrative: last, and honestly

News and sentiment are the obvious extension and the weakest fit for this
architecture. Durability here means re-derivation. GDELT guarantees roughly three
months of history and CryptoPanic's free tier is capped. A source you cannot
rebuild is one you lose every Sunday and can never backtest.

If it is wanted anyway, the honest form is a **forward-only** table with a stated
start date, marked as not re-derivable, rather than a pretence that the weekly
rebuild covers it.

### 2.5 On-chain: rejected on availability

Exchange netflows and stablecoin supply would add to the positioning picture.
Their history sits behind a paid tier, which the re-derivation model cannot
rebuild. This is a rejection on availability rather than on merit. It changes the
day a durable bucket exists.

## 3. Integrations worth making

### 3.1 Stage N5, the point-in-time boundary: the real blocker

Everything above adds data. N5 makes the data safe to read. It is specified and
unbuilt: the `*_pit` prepared statements, the tool server as the only IAM
principal that can read Gold, and the lookahead-injection test that must fail
when the filter is removed.

**Until N5 exists, `knowledge_ts` and `vintage_date` are columns nothing
enforces.** The macro work in E3 is exactly the case that needs it. The whole
argument for storing vintages is that a read path filters on them, and no read
path does yet. Build this ahead of any new source.

### 3.2 Maintenance — running

Implemented: [`2026-08-19-iceberg-maintenance-extension.md`](../superpowers/plans/2026-08-19-iceberg-maintenance-extension.md)
extends [`2026-08-17-iceberg-table-maintenance-design.md`](../superpowers/specs/2026-08-17-iceberg-table-maintenance-design.md)
from 3 to 6 tables (adding `silver_perp_context` and `silver_macro`) and runs
`OPTIMIZE`/`VACUUM` as tail states of the state machines that already write
each table. `make maintenance-verify-aws` reports current file, delete-file,
and snapshot counts.

### 3.3 Comparing the two workstreams on one contract

Both workstreams claim to implement the same business use case, and nothing
compares them. One query that reads `gold_bars_1m` from the AWS side and the
Delta equivalent from Databricks for the same instrument-minute, then reports the
difference with coverage, turns that claim into a measurement. It is also the
cheapest item on this list.

### 3.4 Alerting — running

Implemented: [`2026-08-20-iceberg-health-metrics-monitoring.md`](../superpowers/plans/2026-08-20-iceberg-health-metrics-monitoring.md)
publishes freshness, quarantine rate, and file/delete-file/snapshot counts to
CloudWatch (namespace `FDAI/Native`) from a new tail state in each writer
state machine, with alarms on freshness, quarantine rate, and stalled
maintenance, all notifying one SNS topic. History lives in the new
`native_health_metrics` table, which [`2026-08-19-iceberg-housekeeping-monitoring-design.md`](../superpowers/specs/2026-08-19-iceberg-housekeeping-monitoring-design.md)
section 5's QuickSight dashboard reads from — that plan has not been written
yet.

### 3.5 Feature layer, when there is something to feed

`gold.features` stays out of scope until a model consumes it. When it arrives,
its `fidelity` column derives from `source_tier`, which every table in this slice
already carries. The groundwork is done and the table is not needed yet.

## 4. Recommended order

1. **N5, the point-in-time boundary.** It is the difference between holding
   vintage-stamped data and being unable to leak the future. Everything else is
   worth more after it.
2. **Maintenance.** Cheap, already designed, and it prevents a slow degradation
   that presents as a bug in the SQL.
3. **E2, liquidity.** The first enrichment that could justify a real constraint
   on position size rather than more context for the model to read.
4. **Cross-workstream comparison.** One query, and it turns the project's central
   claim into a number.
5. **Settled funding and cross-venue sanity.** Small, additive, and they close
   named gaps rather than opening new ones.

E4 and on-chain sit below all of these for the same reason: this architecture
buys its durability from re-derivation, and a source that cannot be re-derived
costs more than it looks like it costs.

## 5. What this page does not claim

- That anything is deployed. It is not.
- That the enrichment improves decisions. It is informational by decision, and no
  backtest has scored it. A veto threshold invented before that measurement would
  be a rule nobody could defend.
- That the dashboard's numbers are stable. They are whatever the tables held when
  the page was generated, in an account that is wiped every seven days.
