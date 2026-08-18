# Data layer: what exists, and what to extend next

**Date:** 2026-08-18
**Status:** draft, for discussion

A short piece on where the data layer stands after slices E1 and E3, what to add
next, and what to integrate it with. Ordered by value per unit of effort, with
the reason each item sits where it does.

---

## 1. Where it stands

| Layer | Table | Source | Grain |
|---|---|---|---|
| Bronze | `bronze_trades_stream` | Binance + Coinbase WebSocket | per trade |
| Bronze | `bronze_perp_context` | Binance REST, 5-minute poll | per instrument per poll |
| Bronze | `bronze_macro_observations` | ALFRED CSV, daily pull | per series per vintage |
| Silver | `silver_trades` | Bronze, keyed upsert | per trade |
| Silver | `silver_trades_quarantine` | Bronze, complementary predicate | per bad row |
| Silver | `silver_perp_context` | Bronze, insert-only | 5-minute grid |
| Silver | `silver_macro` | Bronze, insert-only per revision | per observation per vintage |
| Silver | `archive_staging_*` | `data.binance.vision` | transient |
| Gold | `gold_bars_1m` | Silver, dirty-partition rebuild | 1 minute |
| Ops | `backfill_manifest` | the loader | per archive file |

The dashboard reads these and answers two questions: is the pipeline running, and
what does the enrichment show that price and size alone cannot.

**Verified offline, not in an account.** Every number in this repo comes from a
real API response or a real archive file, but nothing here has been deployed. The
account is wiped weekly, so the assumptions listed in each design's X-table stay
open until someone runs `make up-aws`.

## 2. Extending the data

### 2.1 E2, liquidity — the gap that matters most

The agent's decision output carries a `size_pct`, and **nothing in the data layer
can say whether that size is executable.** `bookDepth` is already in the archive
at 553 KB per day per symbol, giving depth and notional at ±1% to ±5% from mid.

With it, three things become possible that are not possible now:

- a slippage estimate for a proposed size, so a decision can be costed
- a hard cap the risk engine can enforce, which is the first enrichment that
  would justify moving from informational to enforcing
- a liquidity-adjusted view of the flow imbalance already in Gold

Cost is one more archive tier through a loader that exists. This is the highest
value per unit of effort of anything on this page.

### 2.2 Settled funding, from the archive

`silver_perp_context` holds the funding **quote**, sampled every five minutes. The
monthly `fundingRate` archive files hold what actually settled, with the interval
per symbol. The two answer different questions, and the gap between quote and
settlement is itself a signal. It also resolves an ambiguity recorded rather than
guessed: whether `lastFundingRate` describes the last settlement or the next one.

Small: 914 bytes per month per symbol, and the loader already reads this bucket.

### 2.3 Cross-venue price sanity

Deferred since the first design (§5.4). Coinbase and Binance both feed Silver, so
the data to compare is already there; what is missing is a windowed check against
the cross-venue median, and a quarantine reason for a price that fails it. It
closes the last named hole in the validity contract.

### 2.4 E4, narrative — last, and honestly

News and sentiment are the obvious extension and the weakest fit for this
architecture. Durability here is re-derivation, and GDELT guarantees roughly three
months of history while CryptoPanic's free tier is capped. A source you cannot
rebuild is one you lose every Sunday and can never backtest.

If it is wanted anyway, the honest framing is a **forward-only** table with a
stated start date, marked as not re-derivable, rather than pretending the weekly
rebuild covers it.

### 2.5 On-chain — rejected on availability, revisit only if that changes

Exchange netflows and stablecoin supply would genuinely add to the positioning
picture. Their history sits behind a paid tier, which the re-derivation model
cannot rebuild. This is a rejection on availability rather than on merit; it
changes the day a durable bucket exists.

## 3. Integrations worth making

### 3.1 Stage N5, the point-in-time boundary — the real blocker

Everything above adds data. N5 is what makes the data safe to read. It is
specified and unbuilt: `*_pit` prepared statements, the tool server as the only
IAM principal that can read Gold, and the lookahead-injection test that must fail
if the filter is removed.

**Until N5 exists, `knowledge_ts` and `vintage_date` are columns nothing
enforces.** The macro work in E3 is precisely the case that needs it: the whole
argument for storing vintages is that a read path filters on them, and no read
path does yet. This is the next thing to build, ahead of any new source.

### 3.2 Maintenance, from proposal to running

The maintenance design is written and unimplemented. Three of the four new tables
in this slice are insert-only, so they accumulate small files exactly as that
document predicts, and `gold_bars_1m` accumulates merge-on-read delete files on
every micro-batch. Nothing runs `OPTIMIZE` or `VACUUM` today, so the defaults do
not take effect. The dashboard would show the symptom as a rising query time
before anyone diagnosed the cause.

### 3.3 Comparing the two workstreams on one contract

Both workstreams claim to implement the same business use case. Nothing compares
them. A single query that reads `gold_bars_1m` from the AWS side and the Delta
equivalent from Databricks for the same instrument-minute, and reports the
difference with coverage, would turn that claim into a measurement. It is also
the cheapest thing on this list.

### 3.4 Alerting on the checks that already exist

The quarantine rate, the freshness lag, and the reconciliation coverage are all
computed and all only visible if somebody opens the dashboard. A CloudWatch alarm
on two or three of them costs almost nothing. The rule worth keeping from §6.4:
alert on discrepancy **and** coverage together, so a check that passed over zero
comparable rows reads as "no evidence" rather than as "correct".

### 3.5 Feature layer, when there is something to feed

`gold.features` stays out of scope until a model consumes it. When it arrives, its
`fidelity` column derives from `source_tier`, which every table in this slice now
carries. The groundwork is done; the table is not needed yet.

## 4. What I would do in order

1. **N5, the point-in-time boundary.** It is the difference between having
   vintage-stamped data and being unable to leak the future. Everything else is
   worth more after it.
2. **Maintenance.** Cheap, already designed, and it prevents a slow degradation
   that presents as a bug in the SQL.
3. **E2, liquidity.** The first enrichment that could justify a real constraint on
   position size rather than more context for the model to read.
4. **Cross-workstream comparison.** One query, and it turns the project's central
   claim into a number.
5. **Settled funding and cross-venue sanity.** Small, additive, and they close
   named gaps rather than opening new ones.

E4 and on-chain sit below all of these, for the same reason: this architecture
buys its durability from re-derivation, and a source that cannot be re-derived
costs more than it looks like it costs.

## 5. What this page deliberately does not claim

- That anything is deployed. It is not.
- That the enrichment improves decisions. It is informational by decision, and no
  backtest has scored it. A veto threshold invented before that measurement would
  be a rule nobody could defend.
- That the dashboard's numbers are stable. They are whatever the tables held when
  the page was generated, in an account that is wiped every seven days.
