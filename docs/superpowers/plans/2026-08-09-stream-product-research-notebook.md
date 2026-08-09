# Stream Product Research Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained, read-only notebook that analyzes either the local Kafka trade stream or AWS MSK one target per run, then turns source-quality and market-structure evidence into prioritized data-product recommendations.

**Architecture:** Add one notebook containing configuration, preflight, capture, quality, market, and recommendation sections. Reuse `devlab` for target resolution, bounded Kafka reads, decoding, health checks, deduplication, and bars; keep all new calculations in notebook cells as requested. Verify its static read-only contract and execute it against local Kafka before stripping outputs.

**Tech Stack:** Jupyter nbformat 4, Python 3.12, pandas, NumPy, matplotlib, confluent-kafka through `devlab`, pytest, Ruff.

## Global Constraints

- Create `notebooks/04_stream_product_research.ipynb`; do not modify the user's in-progress `notebooks/03_msk_live_experiment.ipynb`.
- Analyze one explicit target per run: `TARGET = "local"` through `devlab.local()` or `TARGET = "msk"` through `devlab.from_terraform()`.
- Expose `RUN_MODE = "quick"` with limits of 20,000 records/60 seconds and `RUN_MODE = "deep"` with limits of 200,000 records/600 seconds.
- Read only `md.trades.v1` by default. Do not produce, commit offsets, mutate topics or infrastructure, or export captured data.
- Use one bounded captured DataFrame for all downstream experiments.
- Do not expose SASL passwords or raw credentials.
- Treat cross-venue spreads as research observations, not executable arbitrage.
- Keep Coinbase excluded from replay-based sequence-gap conclusions.
- Strip all notebook outputs before committing.

---

### Task 1: Notebook contract and executable research scaffold

**Files:**
- Create: `tests/devlab/test_stream_product_research_notebook.py`
- Create: `notebooks/04_stream_product_research.ipynb`

**Interfaces:**
- Consumes: `devlab.local()`, `devlab.from_terraform()`, `devlab.topics()`, `devlab.partitions()`, `devlab.rate()`, `devlab.collect()`, `frames.frame()`, `frames.trades_frame()`, `frames.dedupe()`.
- Produces: notebook variables `TARGET`, `RUN_MODE`, `TOPIC`, `PROFILE`, `target`, `raw_df`, `clean_df`, and `capture_summary` for later cells.

- [ ] **Step 1: Write failing notebook contract tests**

Create a pytest module that loads the notebook as JSON, concatenates code-cell source, compiles every code cell, and asserts:

```python
NOTEBOOK = Path("notebooks/04_stream_product_research.ipynb")

def test_notebook_has_switchable_bounded_read_only_contract():
    notebook = json.loads(NOTEBOOK.read_text())
    source = "\n".join("".join(c["source"]) for c in notebook["cells"] if c["cell_type"] == "code")
    assert 'TARGET = "local"' in source
    assert 'RUN_MODE = "quick"' in source
    assert '"quick": {"limit": 20_000, "seconds": 60.0}' in source
    assert '"deep": {"limit": 200_000, "seconds": 600.0}' in source
    assert "devlab.local()" in source
    assert "devlab.from_terraform()" in source
    assert 'offset_reset="earliest"' in source
    for forbidden in (".produce(", ".commit(", "create_topics", "to_csv(", "to_parquet("):
        assert forbidden not in source
```

Also assert markdown contains the six named experiment groups and the caveat “not executable arbitrage.”

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/devlab/test_stream_product_research_notebook.py -q`

Expected: FAIL because `notebooks/04_stream_product_research.ipynb` does not exist.

- [ ] **Step 3: Add configuration, preflight, capture, and normalized views**

Create a valid nbformat-4 notebook with:

```python
TARGET = "local"
RUN_MODE = "quick"
TOPIC = "md.trades.v1"
PROFILES = {
    "quick": {"limit": 20_000, "seconds": 60.0},
    "deep": {"limit": 200_000, "seconds": 600.0},
}
if TARGET not in {"local", "msk"}:
    raise ValueError("TARGET must be 'local' or 'msk'")
if RUN_MODE not in PROFILES:
    raise ValueError("RUN_MODE must be 'quick' or 'deep'")
PROFILE = PROFILES[RUN_MODE]
target = devlab.local() if TARGET == "local" else devlab.from_terraform()
```

Preflight uses bounded calls to render topics, partitions, and a short live rate. Capture uses:

```python
records = devlab.collect(
    target,
    TOPIC,
    limit=PROFILE["limit"],
    seconds=PROFILE["seconds"],
    offset_reset="earliest",
)
raw_df = frames.trades_frame(records)
if raw_df.empty:
    raise RuntimeError("No trades were captured; verify the preflight and producer before continuing.")
clean_df = frames.dedupe(raw_df)
```

Create `capture_summary` containing target, mode, capture limits, rows, event-time start/end/duration, venues, and instruments.

- [ ] **Step 4: Run contract tests to verify GREEN**

Run: `uv run pytest tests/devlab/test_stream_product_research_notebook.py -q`

Expected: all contract and compilation tests pass.

- [ ] **Step 5: Commit task 1**

```bash
git add notebooks/04_stream_product_research.ipynb tests/devlab/test_stream_product_research_notebook.py
git commit -m "feat: scaffold stream product research notebook"
```

### Task 2: Deep quality and market experiments

**Files:**
- Modify: `notebooks/04_stream_product_research.ipynb`
- Modify: `tests/devlab/test_stream_product_research_notebook.py`

**Interfaces:**
- Consumes: `records`, `raw_df`, `clean_df`, `capture_summary`, `health.sequence_gaps()`, `frames.venue_comparison()`.
- Produces: `coverage`, `latency_summary`, `quality_metrics`, `activity`, `volatility_ranking`, `spread_summary`, `leadership`, `recommendations`.

- [ ] **Step 1: Extend failing tests for experiment coverage and invariants**

Assert the notebook code/markdown exposes all required results and inline invariants:

```python
for result in (
    "coverage", "latency_summary", "quality_metrics", "activity",
    "volatility_ranking", "spread_summary", "leadership", "recommendations",
):
    assert result in source
for invariant in (
    "assert (clean_df[\"size\"] >= 0).all()",
    "assert not clean_df.duplicated(subset=NATURAL_KEY).any()",
    "assert (clean_df[\"notional\"] >= 0).all()",
):
    assert invariant in source
```

Expected mutation caught: removing any experiment result or read-only invariant fails the contract test.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/devlab/test_stream_product_research_notebook.py -q`

Expected: FAIL because the scaffold does not yet define all named experiments.

- [ ] **Step 3: Add source-quality experiments**

Add cells that calculate:

- `coverage`: rows, base volume, notional, and observed event-time range by venue/instrument;
- null counts/rates, source/backfill shares, and schema dtypes;
- latency median/p90/p95/p99/max overall and by venue, negative counts, extreme-tail counts, and end freshness;
- raw duplicate rate using `NATURAL_KEY`, conflicting duplicates across price/size/side/event time, and deduplicated invariants;
- valid sequence-gap report through `health.sequence_gaps(records)`;
- count/notional partition distribution and coefficient of variation;
- per-symbol event-time regressions using capture order from `records`; and
- per-partition offset regressions using capture order.

Place compact interpretation markdown after each result, including why Coinbase replay gaps are excluded.

- [ ] **Step 4: Add market-activity experiments**

Using `clean_df`, add:

- per-minute trades/notional with plots;
- trade-size and notional percentiles;
- buy/sell count and notional imbalance;
- event-time price buckets, returns, rolling volatility in basis points, minimum-bucket guards, and rankings; and
- active-instrument rankings based separately on trades and notional.

Do not annualize volatility. Every ranking includes its number of observations.

- [ ] **Step 5: Add cross-venue structure experiments**

Use `frames.venue_comparison(clean_df, freq=BUCKET_FREQ)` for synchronized VWAP/spread analysis. Summarize spread median/p90/p95/min/max, sign persistence, and sample count per instrument. For instruments with at least ten synchronized return buckets, evaluate correlations at lags `-2..2`, define the lag direction in markdown, and produce `leadership`; otherwise return an explanatory empty DataFrame.

- [ ] **Step 6: Add evidence-backed data-product recommendations**

Build a list through a local helper that requires non-empty `metric`, `measured_value`, `evidence_strength`, `proposed_product`, `layer`, `validation_or_sla`, and `priority`. Generate recommendations for Bronze metadata, Silver dedupe/conflict quarantine, freshness/latency observability, partition skew, event-time bars, activity/liquidity, volatility/imbalance features, and cross-venue spreads when their evidence exists.

End with:

```python
recommendations = pd.DataFrame(recommendation_rows)
assert not recommendations.empty
assert recommendations["metric"].str.len().gt(0).all()
assert recommendations["measured_value"].notna().all()
recommendations.sort_values(["priority", "layer"])
```

- [ ] **Step 7: Run tests to verify GREEN**

Run: `uv run pytest tests/devlab/test_stream_product_research_notebook.py -q`

Expected: all tests pass.

- [ ] **Step 8: Commit task 2**

```bash
git add notebooks/04_stream_product_research.ipynb tests/devlab/test_stream_product_research_notebook.py
git commit -m "feat: add stream quality and market experiments"
```

### Task 3: Documentation and executable verification

**Files:**
- Modify: `notebooks/README.md`
- Modify: `notebooks/04_stream_product_research.ipynb`
- Test: `tests/devlab/test_stream_product_research_notebook.py`

**Interfaces:**
- Consumes: completed notebook and local broker at `127.0.0.1:9092`.
- Produces: documented notebook workflow with stripped outputs and verified local execution.

- [ ] **Step 1: Add the notebook to user documentation**

Add the notebook to the README table and document:

```text
TARGET = "local" | "msk"
RUN_MODE = "quick" | "deep"
```

State that one target is analyzed per run, the notebook is strictly read-only, and MSK resolution uses live Terraform outputs.

- [ ] **Step 2: Validate notebook structure and static quality**

Run:

```bash
uv run pytest tests/devlab/test_stream_product_research_notebook.py -q
uv run ruff check notebooks/04_stream_product_research.ipynb tests/devlab/test_stream_product_research_notebook.py
uv run ruff format --check tests/devlab/test_stream_product_research_notebook.py
uv run python -m json.tool notebooks/04_stream_product_research.ipynb >/dev/null
```

Expected: every command exits zero.

- [ ] **Step 3: Execute locally using the quick profile**

Ensure local Kafka and topics exist:

```bash
docker compose -f docker/compose.yaml up -d --wait kafka
uv run python -m scripts.create_topics --bootstrap 127.0.0.1:9092 --replication-factor 1
```

Start the producer in a separate process, execute with a generous timeout, then stop the producer cleanly:

```bash
INGEST_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run python -m ingest.cli
uv run --group notebook jupyter nbconvert --to notebook --execute notebooks/04_stream_product_research.ipynb --output /tmp/04_stream_product_research.executed.ipynb --ExecutePreprocessor.timeout=240
```

Expected: notebook execution exits zero and contains no error outputs.

- [ ] **Step 4: Strip outputs and re-run static checks**

Run:

```bash
uv run --group notebook nbstripout notebooks/04_stream_product_research.ipynb
uv run pytest tests/devlab/test_stream_product_research_notebook.py -q
uv run ruff check notebooks/04_stream_product_research.ipynb tests/devlab/test_stream_product_research_notebook.py
git diff --check
```

Expected: zero output cells, tests and lint pass, diff is clean.

- [ ] **Step 5: Commit task 3**

```bash
git add notebooks/04_stream_product_research.ipynb notebooks/README.md tests/devlab/test_stream_product_research_notebook.py
git commit -m "docs: add stream product research workflow"
```

## Final verification

Run:

```bash
make notebook-test
uv run ruff check .
uv run mypy ingest devlab
git status --short
```

Expected: notebook tests, lint, and type checking pass. The only unrelated working-tree change remains the user's pre-existing `notebooks/03_msk_live_experiment.ipynb` edit.
