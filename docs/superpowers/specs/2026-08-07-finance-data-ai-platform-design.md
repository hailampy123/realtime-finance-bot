# Finance Data + AI Platform — Design

**Date:** 2026-08-07
**Status:** Approved (sections 1–2 reviewed; 3–5 drafted under blanket approval)

## 1. Purpose and constraints

A production-shaped platform for crypto and equity market data that streams live prices
and news into a lakehouse, computes point-in-time features, and drives an LLM-first
trading-decision agent. Built to practice AWS, Databricks, Kafka, and Spark Structured
Streaming on a use case where correctness actually matters.

### Hard constraints

| Constraint | Consequence |
|---|---|
| AWS sandbox account is **wiped every 7 days**, all resources | Account A holds nothing durable; full rebuild must be one command |
| **No IAM role creation** in the sandbox (SCP-restricted) | No Databricks-on-AWS workspace, no Lambda, no ECS tasks, no instance profiles. Every auth path must be non-IAM |
| Databricks workspace lives in a **separate, permanent** AWS account, user has workspace admin | Unity Catalog managed Delta is the system of record |
| Portfolio-grade, but must scale if requirements grow | Modular Terraform, env-parameterized sizing, no hardcoded scale |
| Paper trading only | Risk engine and guardrails are still built as if real, but execution is simulated |

### Non-goals

- Real capital deployment. There is no live-trading flag, deliberately.
- Sub-millisecond latency. This is a minutes-to-seconds system, not an HFT stack.
- Full US equity market coverage. Free equity data is IEX-only (~2% of volume); crypto
  carries the real streaming workload.

## 2. Architecture

Two accounts. **Kafka is the cross-account integration contract** — the only thing that
crosses the boundary. Databricks in account B cannot read S3 in account A (that needs an
IAM role in A, which cannot be created), but it can consume a Kafka topic over a public
endpoint with SASL/SCRAM.

```text
ACCOUNT A (sandbox — ephemeral, wiped weekly, no IAM roles)
┌──────────────────────────────────────────────────────────┐
│  EC2 (no instance profile)                               │
│   └── docker: producers ── Binance/Coinbase/Kraken WS    │
│                         ── Alpaca IEX WS                 │
│                         ── Alpaca news WS / Finnhub poll │
│                              │ SASL/SCRAM                │
│  MSK provisioned (2× t3.small, public access)  ◄─────────┤
│  Secrets Manager (SCRAM creds) + customer-managed KMS    │
│  S3 (Terraform state only — disposable)                  │
└──────────────────┬───────────────────────────────────────┘
                   │ public bootstrap endpoint,
                   │ SG-allowlisted to the workspace NAT EIP
                   ▼
ACCOUNT B (Databricks — permanent, system of record)
┌──────────────────────────────────────────────────────────┐
│  Structured Streaming (kafka source, Trigger.AvailableNow)│
│   → Bronze Delta → Silver → Gold                         │
│  MLflow · Unity Catalog · Databricks Apps                │
└──────────────────────────────────────────────────────────┘
```

### Why this shape

- **Nothing durable lives in the account that gets wiped.** The weekly reset stops being
  a disaster and becomes a routine `make down` / `make up`.
- **Streaming is scheduled micro-batch by default** (`Trigger.AvailableNow` every 5 min),
  not a 24/7 continuous stream. Kafka's 24h retention bridges the gaps, DBU spend tracks
  real work, and the design survives the weekly wipe gracefully. A
  `streaming_mode = "continuous" | "triggered"` flag flips to always-on for demos.

### Hedges against unverified constraints

1. `kafka_backend = "msk" | "ec2"` — if MSK is blocked by SCP or public access proves
   unworkable, a single-broker Kafka (KRaft) on EC2 substitutes. Same client code, same
   topics, same Spark reader; only the Terraform module changes.
2. Every IAM role ARN is a `var.*` input defaulting to `null`. Modules fall back to
   non-IAM auth when null. If a pre-provisioned `LabRole`-style role exists, passing it in
   yields the production-correct path with no code change.

### Carried assumptions (unverified, non-blocking)

- Databricks tier is Premium or above with Unity Catalog enabled, and secret scopes can be
  created. If UC is unavailable, tables land in `hive_metastore` — a weaker governance
  story, not a blocker.
- The Databricks workspace has outbound internet through a NAT gateway with a stable EIP
  to allowlist. If it does not, fall back to approach B (see §11).

## 3. Repository layout

```text
infra/            Terraform, account A only
  bootstrap/      TF state backend (S3 + DynamoDB lock) — solves the chicken-and-egg
  modules/
    network/      VPC, subnets, SGs, IGW
    kafka_msk/    MSK provisioned + SASL/SCRAM + KMS + Secrets Manager
    kafka_ec2/    fallback backend: Kafka KRaft single broker on EC2
    producer_host/ EC2 + user-data docker compose
  envs/dev/       tfvars; role ARNs are inputs, never resources
ingest/           Python package
  connectors/     one module per venue; thin adapters over core/
  core/           ws client, reconnect, backpressure, rate limiting, gap tracking
  schemas/        *.avsc — single source of truth for producer AND Spark reader
  cli.py
backfill/         Re-hydration from data.binance.vision + vendor REST
lakehouse/        Databricks Asset Bundles (databricks.yml), pipelines, jobs
  bronze/ silver/ gold/
  ref/            instrument mapping, calendars
ai/               Decision agent, tool server, eval harness
serving/          FastAPI tool server + decision API; Databricks App dashboard
config/           universe.yaml, risk_limits.yaml
docker/           compose: local Kafka + producers + replay feed
tests/
Makefile          up / down / rebuild / smoke
docs/superpowers/specs/
```

Module boundaries are chosen so each unit is testable alone: a connector knows one
exchange's wire format and nothing else; `core/` knows resilience and nothing about
exchanges; `schemas/` is data, shared by both sides of the wire.

## 4. Reproducibility contract

This is the load-bearing consequence of the 7-day wipe and belongs at the top of the README.

- `make up`: bootstrap state backend → `terraform apply` → generate SASL credentials →
  push broker endpoints + credentials into a Databricks secret scope → create topics →
  launch producers → smoke test. **One command, empty account to streaming data, target
  under 20 minutes.**
- `make down`: destroys account A entirely. Databricks untouched.
- **Any manual console step is a bug.** If `make up` can't create it, it doesn't exist.
- Terraform state lives in S3 *inside account A*. Losing state is normally a disaster; here
  the loss is **atomic with the resources**, so state and reality stay consistent (both
  empty). This looks wrong at first glance and must be documented explicitly.
- Broker DNS changes on every rebuild. Databricks never hardcodes it — it reads the secret
  scope that the bootstrap script rewrites.
- No secrets in git. SASL passwords are generated at apply time and pushed to both Secrets
  Manager and the Databricks secret scope.

## 5. Ingestion layer

### Topics

| Topic | Key | Partitions | Retention | Notes |
|---|---|---|---|---|
| `md.trades.v1` | `{venue}\|{symbol}` | 6 | 24h | Primary stream; key gives per-instrument-per-venue ordering |
| `md.book.top.v1` | `{venue}\|{symbol}` | 6 | 6h | Best bid/ask only |
| `md.book.depth.v1` | `{venue}\|{symbol}` | 6 | 2h | **Off by default** — ~10× trade volume; opt-in per symbol |
| `md.bars.v1` | `{venue}\|{symbol}` | 3 | 48h | Exchange 1m klines; free cross-check on own aggregation |
| `news.articles.v1` | `{source}\|{id}` | 3 | 7d | Alpaca/Benzinga news WS + Finnhub REST poll |
| `ops.metrics.v1` | `{component}` | 1 | 7d | Producer telemetry (see §9) |
| `_dlq.<topic>` | — | 1 | 7d | Unparseable payloads with raw bytes + exception |

Sizing: ~10 crypto pairs + ~10 large-cap equities ≈ 200–400 msg/s, ~150 bytes/record Avro
→ **~5 GB/day**. Provision **50 GB EBS per broker** (2× `t3.small`), which covers 24h
retention with headroom for a depth-topic experiment.

**Teardown loses in-flight Kafka data by design.** `make down` destroys the brokers, so any
records not yet consumed into Bronze are gone. This is acceptable and intentional: the
backfill path (§6) re-derives the missing window from public archives, and the
reconciliation job proves it converged. The operational rule is to let the Databricks job
run once before tearing down; if it doesn't, backfill catches it the next day.

Known skew: BTCUSDT dominates one partition. Acceptable at this scale; documented rather
than pre-optimized.

### Schema strategy — Avro without a registry

Glue Schema Registry needs IAM that can't be created; a self-hosted registry dies weekly
and would need public exposure. Instead:

- Avro payloads; schemas in `ingest/schemas/*.avsc`, versioned in git.
- Kafka header `schema_version` selects the schema at read time; Spark uses
  `from_avro(data, jsonFormatSchema)` with the `.avsc` packaged into the Databricks bundle.
- **The same file is producer and consumer truth**, so drift is impossible by construction.
- CI asserts backward compatibility between adjacent schema versions — a breaking change
  fails the build, not a 3am streaming job.

### Delivery semantics

At-least-once to Kafka (`acks=all`, `enable.idempotence=true`) plus **idempotent writes
keyed on a natural key**:

| Venue | Natural key |
|---|---|
| Binance | trade id `t` |
| Coinbase | `trade_id` |
| Kraken | `sha1(symbol, ts_ns, price, size, side)` (no stable id published) |

Duplicates become harmless, which is what makes backfill-over-live convergent.

### Gap detection and repair

A WebSocket that silently misses 40 seconds of trades is the classic market-data failure
and is invisible unless explicitly detected.

- Producers track last sequence per `(venue, symbol)` — Binance trade ids and depth
  `U`/`u`, Coinbase `sequence_num` — and emit a `gap_detected` metric plus a marker record
  on any jump.
- On reconnect, the producer **repairs the gap over REST** (e.g. Binance
  `aggTrades?fromId=`) and republishes to the same topic with `is_backfill=true`. Overlap
  is absorbed by natural-key dedupe.
- Binance expires WS connections at 24h; producers reconnect proactively at ~23h.
- Order books re-snapshot via REST on reconnect and validate deltas against the snapshot
  sequence (standard depth-management algorithm).

### Backpressure policy (explicit, per topic)

Bounded asyncio queue. When full:

- **Depth updates are dropped** — recoverable from the next snapshot.
- **Trades are never dropped** — the producer blocks, takes the resulting gap, and repairs
  it via REST. Silent trade loss is the one failure this system must not have.

### Rate limiting

Per-venue token bucket in `core/ratelimit.py` — Coinbase 10 rps public, Kraken ~1 rps
public, Binance weight-based (6000/min spot). REST is used only for gap repair and
snapshots, so the budget is small, but exceeding it triggers an IP ban.

### Symbol universe

`config/universe.yaml`. Widening coverage never touches connector code.

## 6. Lakehouse

### Bronze — `bronze.md_trades_raw`, `bronze.news_raw`, `bronze.ops_metrics_raw`

Append-only, partitioned by `ingest_date`. Preserves Kafka metadata (`topic`, `partition`,
`offset`, `kafka_timestamp`) alongside the raw value bytes and the parsed struct, plus
`ingest_ts`, `schema_version`, `is_backfill`, `source` (`stream` | `archive`).

Reader config: `kafka` source, `SASL_SSL` / `SCRAM-SHA-512`, bootstrap servers and
credentials from the Databricks secret scope, `maxOffsetsPerTrigger` to bound batch size,
checkpoint in a UC volume. `Trigger.AvailableNow`, scheduled every 5 minutes by a
Databricks Job.

### Silver — `silver.trades`, `silver.book_top`, `silver.news`

Typed, deduplicated on natural key with a 10-minute watermark, timestamps normalized to
UTC microseconds, venue symbols mapped to a canonical instrument id via `ref.instruments`
(so `BTCUSDT`, `BTC-USD`, `XBT/USD` collapse to one instrument).

Data quality enforced as Lakeflow/DLT expectations:

- `price > 0`, `size > 0`
- `event_ts` within `[now - 1d, now + 1m]`
- cross-venue price sanity: deviation from the cross-venue median beyond a threshold

**Violations are quarantined to `silver.trades_quarantine`, never dropped.** Silent
discard destroys the ability to explain a gap later.

### Gold — `gold.bars_1m`, `gold.bars_1s`, `gold.features`

- Bars: OHLCV, VWAP, trade count, buy/sell volume imbalance; watermarked window aggregation.
- Features: rolling returns, realized volatility, RSI/MACD-family indicators, order-book
  imbalance, trade-flow imbalance.
- **Point-in-time correctness:** every feature row carries `as_of_ts` and is computed only
  from rows with `event_ts <= as_of_ts`. This is the anti-lookahead discipline that makes
  every downstream backtest honest.

### Backfill and re-hydration

The durability strategy is *re-derivation*, not backup.

- `backfill/` downloads Binance public archives (`data.binance.vision`, daily/monthly ZIPs
  of trades, aggTrades, and klines down to 1s) and writes to Bronze with
  `source='archive'`.
- Same natural key → dedupe → converges with live data.
- A `backfill_manifest` Delta table records `(symbol, date, status, checksum)` so re-runs
  skip completed partitions. Idempotent and resumable.
- Live streaming covers the window between archive publication (next-day) and now.

### Reconciliation — the correctness proof

A nightly job compares `gold.bars_1m` built from the live stream against the exchange's own
published klines for the prior day, and emits a discrepancy metric. This turns "my
streaming pipeline is correct" from a claim into a measurement, and it is the single most
convincing artifact in the whole project.

## 7. AI decision layer

LLM-first: a Claude agent is the product; quant features and models are tools it queries.

### Model configuration

- `claude-opus-5`, adaptive thinking (on by default on this model).
- `output_config.effort` swept per route — start at `high`, test `medium` and `low`;
  `low`/`medium` are unusually strong on this model and are the main cost lever.
- Handle `stop_reason == "refusal"` before reading content; opt into
  `fallbacks: "default"` (beta `server-side-fallback-2026-07-01`).

### Structured output contract

Recommendations are produced via `output_config.format` with a JSON schema, not parsed out
of prose:

```text
action                 BUY | SELL | HOLD | FLAT
conviction             0.0–1.0
horizon                e.g. "4h", "1d", "1w"
size_pct               proposed % of risk budget
rationale              prose
key_risks[]            what would make this wrong
evidence[]             { source, ref, timestamp } — every claim cited with a timestamp
invalidation_condition the observable that should close the position
```

### Tools

Behind a thin FastAPI tool server backed by Databricks SQL:

| Tool | Returns |
|---|---|
| `get_price_context(symbol, as_of, lookback)` | OHLCV, returns, realized vol |
| `get_features(symbol, as_of)` | point-in-time feature vector |
| `get_news(symbol, as_of, since)` | scored news articles |
| `get_quant_signal(symbol, as_of)` | registered model's forward-return prediction |
| `get_portfolio_state(as_of)` | positions, cash, exposure |
| `get_risk_limits()` | caps and halts |
| `search_history(query)` | RAG over past decisions and their realized outcomes |

**Anti-lookahead is enforced in the tool layer, not the prompt.** Every tool takes `as_of`
and filters `event_ts <= as_of` in SQL. The model physically cannot see the future because
the data layer will not serve it. Instructing the model not to look ahead would be the
wrong control — a prompt is not an access boundary.

### Prompt caching

Stable prefix first (strategy doctrine, risk policy, output contract, tool definitions),
volatile market snapshot last. Opus 5's minimum cacheable prefix is 512 tokens, so even a
modest doctrine block caches. Verified by asserting `cache_read_input_tokens > 0` in the
eval harness — a silent cache miss is otherwise invisible.

### Backtesting via the Batch API

Replay historical `as_of` timestamps, one request per decision point, `custom_id =
"{symbol}|{as_of_ts}"`, results keyed by `custom_id` (never by position — batch results
return in arbitrary order). 50% cost reduction, which matters: a 2-year daily backtest over
10 symbols is ~5,000 decisions.

### Supporting quant model

Gradient-boosted classifier over `gold.features` predicting the sign of forward return at
horizon `h`. MLflow-tracked, registered in Unity Catalog. **Walk-forward cross-validation
with purging and embargo** — not random k-fold, which leaks across the time axis. Exposed
to the agent as `get_quant_signal`.

### Evaluation

Three layers, because "the LLM said something plausible" is not evaluation:

1. **Deterministic:** output validates against schema; every `evidence[].timestamp <=
   as_of`; `size_pct` within risk limits; cache hit rate non-zero.
2. **Outcome:** forward return at horizon vs. recommendation, hit rate, and a Brier score
   on conviction — an agent whose 0.9-conviction calls hit 55% of the time is miscalibrated
   and should be reported as such.
3. **Baseline:** vs. buy-and-hold, vs. the quant model alone, vs. random.
   **If the agent does not beat buy-and-hold, the report says so.** Publishing an honest
   negative result is a stronger portfolio signal than a suspiciously good backtest.

### Guardrails

- Paper trading only. There is no `LIVE_TRADING_ENABLED` flag to accidentally flip.
- A deterministic risk engine can veto the model: position caps, max-drawdown halt,
  correlation limits, per-asset exposure. **The LLM proposes; code disposes.**
- Every decision persisted to `gold.decisions` with prompt hash, model id, effort, tool
  calls, and realized outcome — fully auditable after the fact.

## 8. Serving

- **Tool server + decision API:** FastAPI, containerized, deployed as a Databricks App in
  account B so it survives the weekly wipe and sits next to the data.
- **Dashboard:** Databricks App (Streamlit) — positions and simulated P&L, decision log
  with rationale and outcome, data-quality panel, pipeline latency panel, spend panel.

## 9. Observability

Producer telemetry is published to `ops.metrics.v1` and lands in Bronze, so **ops data
survives the weekly wipe along with everything else** — a useful side effect of routing
everything through Kafka rather than scraping a Prometheus endpoint on a doomed EC2 box.

Key SLIs:

| SLI | Target |
|---|---|
| WebSocket uptime per venue | > 99% |
| Detected gaps per hour | tracked; every gap must have a matching repair |
| Producer → Bronze end-to-end lag | p50 < 6 min, p99 < 15 min (triggered mode) |
| DQ quarantine rate | < 0.1% of records |
| Live-vs-archive bar discrepancy | < 0.01% of bars |
| Daily DBU spend | under budget alert threshold |

Alerting via Databricks SQL alerts.

## 10. Cost model and guards

| Item | Estimate |
|---|---|
| MSK 2× `kafka.t3.small` | ~$0.09/h |
| EC2 `t3.small` producer host | ~$0.02/h |
| Account A at ~30 h/week | **~$15/mo** |
| Databricks DBUs (job clusters, auto-terminate) | ~$20–40/mo |
| Anthropic API (Batch for backtests) | variable; Batch halves it |
| **Total, with discipline** | **~$40–80/mo** |

Guards: TTL tags on all account-A resources; `make down` as routine; job clusters only with
10-minute auto-termination; spot instances; a cluster policy capping node types;
`Trigger.AvailableNow` on a schedule rather than continuous streaming.

## 11. Rejected alternatives

- **Kinesis + Firehose → S3.** Firehose delivery requires a service IAM role that cannot be
  created; S3 in account A is wiped anyway; and it teaches less Kafka.
- **Databricks Free Edition as the lakehouse.** Serverless-only, one 2X-Small warehouse,
  max 5 concurrent job tasks, one active Lakeflow pipeline per type, outbound access
  restricted to a trusted-domain allowlist (which blocks arbitrary Kafka brokers), and
  accounts may be deleted after prolonged inactivity. Superseded once a permanent workspace
  was confirmed.
- **Databricks-on-AWS in the sandbox.** Workspace creation requires a cross-account IAM
  role. Impossible here.
- **Approach B — Kafka private, consumer pushes Parquet to a UC Volume via the Files API,
  Auto Loader ingests.** Cheaper, no public brokers, works on Serverless. Retained as the
  documented fallback if the workspace has no stable NAT EIP to allowlist or MSK public
  access proves unworkable. Rejected as primary because it never exercises Spark's Kafka
  source — the most transferable piece of the stack.

## 12. Testing strategy

| Layer | What |
|---|---|
| Unit | Connector parsers against committed golden files of real WebSocket frames |
| Contract | Schema backward-compatibility; producer output validates against `.avsc` |
| Integration | docker-compose Kafka + producer + replay feed; assert records land, gaps are detected, dedupe converges |
| Data quality | DLT expectations, plus the nightly live-vs-archive reconciliation |
| Backtest integrity | A deliberate lookahead-injection test that **must** fail the guard — proving the guard works, rather than assuming it |
| Infra | `terraform validate`, `tflint`, `checkov` |
| CI | GitHub Actions running all of the above plus `databricks bundle validate` |

## 13. Build order

Each stage is independently valuable and independently demoable.

| # | Stage | Ships |
|---|---|---|
| 0 | Foundation: repo, CI, Terraform skeleton, `make up`/`down` | Deployable empty platform |
| 1 | **Streaming ingestion** | Live crypto + equity ticks in Kafka, gap-repaired |
| 2 | **Lakehouse medallion** | Bronze→Silver→Gold, DQ gates, reconciliation proof |
| 3 | **Backfill + features** | Full history re-hydrated; point-in-time feature store |
| 4 | **Quant model** | MLflow-tracked, walk-forward-validated signal |
| 5 | **AI decision agent** | Claude agent + tools + eval harness + honest backtest |
| 6 | **Serving + ops** | Dashboard, paper-trading loop, alerting, cost guards |

Stages 0 and 1 are the first implementation plan. Everything after gets its own
spec → plan → build cycle.
