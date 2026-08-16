# finance_data_ai

Streaming market-data and LLM trading-decision platform.
Design: [`docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`](docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md)
Setup, prerequisites, manual AWS/Databricks steps, data sources, and config variables: [`docs/SETUP.md`](docs/SETUP.md)
Current-state diagrams (data flow + deployed AWS topology, built vs. designed-only): [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
Post-deployment AWS verification and debugging commands: [`docs/AWS_DEPLOYMENT_DEBUGGING.md`](docs/AWS_DEPLOYMENT_DEBUGGING.md)

## The reproducibility contract

The AWS sandbox account is wiped every 7 days, so **nothing durable lives in
it**. Unity Catalog managed Delta in the permanent Databricks account is the
system of record; the sandbox is pure ephemeral compute.

- `make up` — empty AWS account to streaming data. Budget 45–60 minutes: three
  serial MSK operations (create, then the ACL configuration and public access
  that MSK requires in that order — see [`docs/SETUP.md`](docs/SETUP.md) §5f).
- `make down` — destroys the sandbox. Databricks is untouched.
- `make rebuild` — both, in order.
- `make unlock` — recovery hatch if a bootstrap dies midway and leaves the
  cluster denying every client.
- **Any manual console step is a bug.** If `make up` can't create it, it doesn't exist.

Terraform state lives in S3 *inside the sandbox account*. Losing state is
normally a disaster; here the loss is **atomic with the resources**, so state
and reality stay consistent (both empty). This looks wrong at first glance —
it isn't.

## The AWS-native workstream

A second implementation of the same use case on AWS-managed services —
Fargate → Kinesis → Firehose → Parquet on S3, queried with Athena. Shares
`ingest/`, `config/universe.yaml`, and `ingest/schemas/trade.v1.avsc` with the
Kafka/Databricks path and diverges below the sink.

Design: [`docs/superpowers/specs/2026-08-14-aws-native-workstream-design.md`](docs/superpowers/specs/2026-08-14-aws-native-workstream-design.md)
Build plan: [`docs/superpowers/plans/2026-08-14-aws-native-n0-n1.md`](docs/superpowers/plans/2026-08-14-aws-native-n0-n1.md)

```bash
make preflight-aws   # prove the account permits every service this needs
make up-aws          # empty account to trades in Bronze, a few minutes
make logs-aws        # follow the producer
make down-aws        # destroy it; the Kafka stack is untouched
```

Both stacks can be up at once: separate VPCs, separate Terraform state keys in
the same bucket, and a `fdai-native-*` naming prefix. Verify the data with the
queries in [`awsnative/sql/verify_bronze.sql`](awsnative/sql/verify_bronze.sql).

Stages N2–N6 (Silver, Gold, backfill, point-in-time serving, agent) are
designed but not yet built — see §12 of the design.

## Prerequisites

- AWS credentials for the sandbox account (`AWS_PROFILE` or env vars)
- `terraform` >= 1.9, `uv`, `docker`, `databricks` CLI authenticated to the workspace
- `cp infra/envs/dev/terraform.tfvars.example infra/envs/dev/terraform.tfvars` and fill in
  `repo_url` and `kafka_client_cidrs` (the Databricks workspace NAT EIP; your
  laptop IP is detected automatically)

## Local development

```bash
make compose-up        # single-broker Kafka on localhost:9092 (empty: no topics, no data)
make check             # lint + typecheck + unit tests
make test-integration  # end-to-end against the local broker
make compose-down
```

To get *data* locally, not just a broker — brings up compose Kafka, creates the
topics, and runs the Binance + Coinbase producers on the host. Runs in the
foreground; leave it in its own terminal:

```bash
make stream-local
```

The producers run on the host rather than in the `producers` container
deliberately: see Known limitations for why the container cannot reach the
broker.

Run producers against local Kafka without touching AWS (see Known limitations —
this currently does not work due to a Docker networking gap):

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

## Consuming trade data

After `make up` (or `make compose-up` locally), trades are flowing but nothing
is yet reading them into a lakehouse — Stage 2 (Databricks Bronze/Silver/Gold)
is designed, not implemented (see
[`docs/superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md`](docs/superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md)).
Three ways to consume in the meantime:

**Local dev notebooks** ([`notebooks/`](notebooks/README.md)) — the interactive
loop: stream health and sequence gaps, exploratory analysis over a bounded
window, and a pandas prototype of the Silver dedupe/bars transforms with the
PySpark each maps to. Backed by [`devlab/`](devlab), a small helper library
that resolves either broker from one env var. Jupyter and pandas live in an
opt-in dependency group, so they never reach `make check` or the producer image.

```bash
make stream-local   # in one terminal
make notebook TARGET=local  # in another
```

For an existing MSK deployment, one command validates/refreshes SSO, updates
the current laptop `/32` through Terraform, verifies Kafka access, and launches
Jupyter with the live Terraform target:

```bash
make notebook TARGET=msk
```

**Ad-hoc local consumer** — decodes the same bare Avro the producer writes and
computes a rolling per-instrument VWAP:

```bash
uv run python -m scripts.consume_example \
  --bootstrap "$(terraform -chdir=infra/envs/dev output -raw bootstrap_brokers_public)" \
  --username  "$(terraform -chdir=infra/envs/dev output -raw sasl_username)" \
  --password  "$(terraform -chdir=infra/envs/dev output -raw sasl_password)"
```

(or `--bootstrap localhost:9092` with no credentials against `make compose-up`).

**Databricks Structured Streaming** (the intended production path once Stage 2
ships) — read the Kafka topic with `spark.readStream.format("kafka")`
authenticated via `kafkashaded.org.apache.kafka.common.security.scram...`
JAAS config sourced from the `${PROJECT}` secret scope (published by `make up`),
decode with `pyspark.sql.avro.functions.from_avro(col("value"), open("ingest/schemas/trade.v1.avsc").read())`
since there's no registry, then transform/aggregate as usual — e.g. a windowed
`groupBy(window("event_ts", "1 minute"), "instrument_id")` for 1-minute bars.

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
- **`docker compose --profile live` cannot deliver messages to the local broker.**
  The Kafka container advertises the host loopback address, which points to
  the `producers` container itself rather than the broker when run from inside a
  sibling container — a known Kafka-in-Docker single-listener limitation.
  Producers work correctly against a real MSK cluster (the actual deployment
  target) and the integration tests (which connect from the host, not from
  inside another container). Fixing this locally would need a second,
  container-internal listener; tracked as a follow-up, not yet implemented.
- **DLQ topics are provisioned but not yet written to.** `_dlq.*` topics exist
  per the design spec's dead-letter contract, but no connector or runner code
  publishes to them yet — an unparseable frame currently propagates as an
  exception, causing the WebSocket session to reconnect rather than being
  routed to the DLQ. Tracked as follow-up work for a later stage.
