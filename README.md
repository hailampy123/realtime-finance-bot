# finance_data_ai

Streaming market-data and LLM trading-decision platform.
Design: [`docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`](docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md)

## The reproducibility contract

The AWS sandbox account is wiped every 7 days, so **nothing durable lives in
it**. Unity Catalog managed Delta in the permanent Databricks account is the
system of record; the sandbox is pure ephemeral compute.

- `make up` — empty AWS account to streaming data. Target: under 20 minutes.
- `make down` — destroys the sandbox. Databricks is untouched.
- `make rebuild` — both, in order.
- **Any manual console step is a bug.** If `make up` can't create it, it doesn't exist.

Terraform state lives in S3 *inside the sandbox account*. Losing state is
normally a disaster; here the loss is **atomic with the resources**, so state
and reality stay consistent (both empty). This looks wrong at first glance —
it isn't.

## Prerequisites

- AWS credentials for the sandbox account (`AWS_PROFILE` or env vars)
- `terraform` >= 1.9, `uv`, `docker`, `databricks` CLI authenticated to the workspace
- `cp infra/envs/dev/terraform.tfvars.example infra/envs/dev/terraform.tfvars` and fill in
  `repo_url` and `kafka_client_cidrs` (the Databricks workspace NAT EIP plus your own IP, each `/32`)

## Local development

```bash
make compose-up        # single-broker Kafka on localhost:9092
make check             # lint + typecheck + unit tests
make test-integration  # end-to-end against the local broker
make compose-down
```

Run producers against local Kafka without touching AWS:

```bash
docker compose -f docker/compose.yaml --profile live up --build
```

## Topics

| Topic | Partitions | Retention |
|---|---|---|
| `md.trades.v1` | 6 | 24h |
| `md.book.top.v1` | 6 | 6h |
| `md.book.depth.v1` | 6 | 2h (off by default) |
| `md.bars.v1` | 3 | 48h |
| `news.articles.v1` | 3 | 7d |
| `ops.metrics.v1` | 1 | 7d |

Records are bare Avro datums (no registry, no magic byte) with the schema
version in a Kafka header. Producer and Spark reader load the same `.avsc` from
`ingest/schemas/`, so drift is impossible by construction.

## Known limitations

- **Teardown loses in-flight Kafka data by design.** `make down` destroys the
  brokers; anything not yet consumed into Bronze is gone. Backfill re-derives it
  from public archives and the reconciliation job proves it converged.
- **Binance gets exact-range gap repair; Coinbase gets best-effort.** Coinbase's
  public market-data API has no id-range query, so repair refetches recent
  trades and relies on natural-key dedupe. Not every venue supports the same
  repair, and hiding that would be worse than documenting it.
- **Equity coverage is IEX-only (~2% of volume)** on Alpaca's free tier. Crypto
  carries the real streaming workload.
