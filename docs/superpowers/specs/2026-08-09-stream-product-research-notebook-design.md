# Stream Product Research Notebook Design

**Date:** 2026-08-09  
**Status:** Approved for implementation planning

## Purpose

Create a self-contained, read-only notebook that examines one live Kafka trade
stream per run and turns the evidence into two outcomes:

1. deeper market understanding; and
2. concrete recommendations for evolving Bronze, Silver, Gold, and monitoring
   data products.

The notebook complements the existing health, exploration, and Silver prototype
notebooks. It provides one end-to-end research narrative rather than replacing
their focused workflows.

## Artifact and scope

Create `notebooks/04_stream_product_research.ipynb`.

All new experiment calculations live in the notebook. It may import existing
`devlab` helpers, pandas, NumPy, and matplotlib, but this iteration does not add
a new Python analysis module. The notebook does not write to Kafka, modify
infrastructure, or export captured data to disk.

## Runtime configuration

The first executable section exposes exactly these primary controls:

```python
TARGET = "local"       # "local" or "msk"
RUN_MODE = "quick"     # "quick" or "deep"
TOPIC = "md.trades.v1"
```

Target selection is explicit and handles one source per run:

- `local` resolves with `devlab.local()`;
- `msk` resolves with `devlab.from_terraform()`; and
- any other value raises a clear `ValueError` before network work begins.

The displayed `Target` must not expose its SASL password.

Capture profiles are fixed defaults selected through `RUN_MODE`:

| Mode | Maximum records | Maximum wall time |
|---|---:|---:|
| `quick` | 20,000 | 60 seconds |
| `deep` | 200,000 | 10 minutes |

Both constraints apply to every capture; the first reached ends the read. The
notebook uses one captured window for every downstream experiment so results
remain internally comparable.

## Notebook narrative and data flow

The notebook follows this execution order:

1. Explain the research questions and limitations.
2. Configure and resolve one target.
3. Run a broker and topic preflight.
4. Capture a bounded trade window.
5. Normalize it with `devlab.frames.trades_frame()`.
6. Establish sample adequacy and a reusable deduplicated view.
7. Run data-quality experiments.
8. Run market-behavior experiments.
9. Translate observations into data-product opportunities.
10. Produce a prioritized recommendation table.

The capture cell reads retained data with `offset_reset="earliest"`. A separate
preflight rate sample reads from `latest` to distinguish a populated but stopped
stream from a stream receiving new data.

## Preflight

Before the expensive capture, the notebook reports:

- selected target name and redacted endpoint configuration;
- topic existence and retained record count;
- partition low/high watermarks and distribution;
- live arrival rate by venue and instrument over a short bounded sample; and
- whether the requested profile is sensible for the available retained data.

Preflight failures preserve the actionable errors already supplied by
`devlab`, including local startup, MSK credentials, Terraform state, current
endpoint, and public-IP allowlist guidance.

## Captured views

The capture produces:

- `raw_df`: typed, event-time-sorted trades from
  `devlab.frames.trades_frame()`; and
- `clean_df`: `raw_df` deduplicated by the natural key
  `(venue, venue_symbol, trade_id)` for market calculations.

Quality experiments use `raw_df` when duplicates and ordering are the subject.
Market experiments use `clean_df` so repaired or replayed records do not inflate
volume, intensity, or VWAP.

## Experiment group 1: source and coverage

Report:

- capture start/end and event-time duration;
- row, venue, instrument, and venue-symbol counts;
- trades, base volume, and notional by venue and instrument;
- missing values by field;
- schema/type consistency; and
- proportions by `source` and `is_backfill`.

The notebook must distinguish raw counts from economic measures: base volume is
not comparable across different instruments, while notional is meaningful only
within the captured quote-currency assumptions.

## Experiment group 2: freshness and latency

Calculate ingest latency as `ingest_ts - event_ts` and report median, p90, p95,
p99, and maximum values:

- overall;
- by venue; and
- by instrument when the sample is adequate.

Identify negative latency and extreme-tail observations separately. Report
freshness at capture completion as the distance between the latest observed
event timestamp and the wall clock. Interpret latency as combined exchange,
network, connector, and local timestamping delay rather than broker latency
alone.

## Experiment group 3: uniqueness and integrity

Measure:

- duplicate natural-key rate;
- duplicates whose price, size, side, or event time conflict;
- sequence gaps using `devlab.health.sequence_gaps()` only where sequence scope
  makes the check valid;
- record and notional distribution across Kafka partitions;
- event-time regressions within each `(venue, venue_symbol)` sequence; and
- offset monotonicity within each partition in the captured read.

Coinbase must remain excluded from Kafka-replay sequence-gap conclusions because
its sequence is connection-scoped while Kafka ordering is partition-scoped.

## Experiment group 4: market activity

Using `clean_df`, calculate and visualize:

- trades and notional per minute;
- trade-size and notional distributions;
- buy/sell trade-count and notional imbalance;
- rolling returns and realized volatility on time-bucketed prices; and
- rankings of active and volatile instruments.

All return and volatility calculations use event-time buckets and state their
window/frequency. Instruments without enough buckets return an explicit
insufficient-sample result rather than zero volatility.

## Experiment group 5: cross-venue market structure

For instruments present on both venues:

- compute per-venue time-bucketed VWAP;
- compute absolute and basis-point spreads;
- summarize spread median, p90, p95, extremes, sign, and persistence;
- compare synchronized venue returns; and
- estimate price leadership with a small, explicit set of lagged return
  correlations.

The notebook labels positive spread observations as research candidates, not
executable arbitrage. It lacks fees, executable depth, transfer constraints,
venue clock guarantees, and order-placement latency.

## Experiment group 6: data-product evolution

Finish with an evidence table whose rows contain:

- observed signal;
- measured value;
- evidence strength or sample limitation;
- proposed product or contract;
- target layer (`Bronze`, `Silver`, `Gold`, or `Observability`);
- proposed SLA or validation rule; and
- priority (`build now`, `validate next`, or `defer`).

Candidate recommendations include, only when supported by the current capture:

- Bronze source and ingestion metadata preservation;
- Silver natural-key deduplication and conflict quarantine;
- event-time quality and freshness metrics;
- per-venue and consolidated bars;
- liquidity/activity profiles;
- cross-venue spread products;
- rolling volatility and imbalance features; and
- operational alerts for latency, gaps, duplicates, freshness, and partition
  skew.

Recommendations must be generated from named notebook metrics. They must not be
presented as universal conclusions when the captured duration, venue coverage,
or sample size is weak.

## Read-only guarantees

The notebook performs no producer, admin mutation, file export, infrastructure
change, checkpoint write, or consumer offset commit. Reads use random consumer
groups with auto-commit disabled through `devlab`.

No credentials or raw secret values appear in cells, outputs, or exception
handling. MSK credentials continue to come from Terraform outputs through
`devlab.from_terraform()`.

## Error handling and evidence thresholds

- Every network read has both a count or operation timeout and a wall-clock
  bound.
- An empty capture stops dependent analysis with a readable instruction.
- Small samples display a warning and suppress unsupported rankings,
  volatility, leadership, and product conclusions.
- Missing venues or instruments yield an explanatory empty result rather than
  a failing pivot.
- Quantiles ignore null values and are shown with their observation counts.
- Plots clip extreme values only for display; reported metrics use unclipped
  data and disclose any clipping.

The notebook records the selected target, profile, capture limits, row count,
event-time duration, venue count, and instrument count near the top of the
results so every interpretation carries context.

## Verification

Before completion:

1. validate notebook JSON and cell ordering;
2. run Ruff against notebook code cells;
3. execute the notebook against the local broker using the quick profile;
4. verify that all cells complete without manual state;
5. assert calculation invariants, including:
   - non-negative trade sizes and derived notional;
   - natural-key uniqueness after deduplication;
   - valid ordered quantiles;
   - VWAP within observed low/high for non-empty buckets;
   - no recommendation row without a named metric/evidence value;
6. strip outputs before committing so live market data and endpoint details do
   not enter version control; and
7. leave MSK execution as an operator validation because it depends on live
   credentials, allowlisting, infrastructure, and external market traffic.

## Documentation updates

Add the notebook to `notebooks/README.md`, document its target/profile controls,
and state that it is a read-only research workflow. Existing focused notebooks
remain supported.

## Non-goals

This iteration does not:

- build production Bronze, Silver, or Gold pipelines;
- persist experiment outputs;
- add order-book or news analysis;
- execute trades or claim executable arbitrage;
- compare local and MSK in one run;
- train predictive models;
- add a reusable `devlab` insights module; or
- change Kafka/MSK infrastructure.
