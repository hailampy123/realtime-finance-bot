# finance_data_ai

Streaming market-data and LLM trading-decision platform. The same use case is
implemented twice, on Kafka with Databricks and on AWS-managed services, over
one shared ingestion contract.

**Where to read next:** [`docs/README.md`](docs/README.md) maps every document
to the question it answers. The short version:

| Question | Document |
|---|---|
| How do I install and configure it? | [`docs/SETUP.md`](docs/SETUP.md) |
| What is built, and what is only designed? | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |
| Where does the data live? | [`docs/DATA_LAYER.md`](docs/DATA_LAYER.md) |
| Which file do I change? | [`docs/CODEBASE_EXPLAINED.md`](docs/CODEBASE_EXPLAINED.md) |
| It broke after a deploy. | [`docs/AWS_DEPLOYMENT_DEBUGGING.md`](docs/AWS_DEPLOYMENT_DEBUGGING.md) |
| Why was it built this way? | [`docs/superpowers/specs/`](docs/superpowers/specs/) |

## The reproducibility contract

The AWS sandbox account is wiped every 7 days, so nothing durable lives in it.
Two rules follow, and they shape every other decision in the repo.

**Any manual console step is a bug.** If a `make` target cannot create it, it
does not exist.

**Durability comes from somewhere else.** On the Kafka path, Unity Catalog
managed Delta in the permanent Databricks account is the system of record. On the
AWS-native path, nothing persists and durability means re-derivation from public
archives.

Terraform state lives in S3 inside the sandbox account. Losing state is normally
a disaster. Here the loss is atomic with the resources, so state and reality stay
consistent, both empty.

## Stack A: Kafka and Databricks

```bash
make up        # empty AWS account to streaming data. Budget 45-60 minutes
make down      # destroy the sandbox. Databricks is untouched
make rebuild   # both, in order
make unlock    # recovery when a bootstrap dies midway and Kafka denies every client
```

`make up` takes 45 to 60 minutes because MSK requires three serial operations:
create the cluster, configure the ACLs, then enable public access, in that order.
[`docs/SETUP.md`](docs/SETUP.md) §5f explains why the order is fixed.

Records on the wire are bare Avro datums with no registry and no magic byte. The
schema version travels in a Kafka header. The producer and the Spark reader load
the same `.avsc` from `ingest/schemas/`, so drift is impossible by construction.

Above Bronze, a Lakeflow pipeline writes `bronze_trades_stream`, `silver_trades`
(keyed upsert on `(venue, trade_id)`), and `silver_trades_quarantine`. It is
triggered rather than continuous. Deploy and validate it with
[`docs/RUNBOOK_STAGE_2A.md`](docs/RUNBOOK_STAGE_2A.md).

```bash
make pipeline-validate   # offline
make pipeline-deploy     # push the bundle
make pipeline-run        # one triggered update
make pipeline-status
```

## Stack B: AWS-native

Fargate to Kinesis to Firehose to Parquet on S3, queried with Athena. Shares
`ingest/`, `config/universe.yaml`, and `ingest/schemas/trade.v1.avsc` with Stack A
and diverges below the sink.

```bash
make preflight-aws   # prove the account permits every service this needs
make up-aws          # empty account to trades in Bronze, a few minutes
make ddl-aws         # create the Iceberg tables (up-aws runs this too)
make microbatch-aws  # run one Bronze -> Silver -> Gold cycle now
make verify-aws      # the acceptance queries, with bytes scanned
make enrich-aws      # run both enrichment collectors now
make dashboard-aws   # build the static dashboard
make logs-aws        # follow the producer
make sfn-logs-aws    # follow the micro-batch
make down-aws        # destroy it; the Kafka stack is untouched
```

One Step Functions state machine runs the whole medallion every five minutes:
Silver and quarantine in parallel from the same Bronze window, then Gold rebuilt
for only the partitions that moved. Silver and Gold are Iceberg on plain S3 and
every transform is an Athena statement, so no code deploys above the producer.

Two scheduled Lambdas add context: perpetual-futures funding and positioning
every five minutes, and six vintage-stamped macro series daily.

Both stacks can be up at once. Separate VPCs, separate Terraform state keys in
the same bucket, and an `fdai-native-` naming prefix.

Stages N4 (backfill), N5 (point-in-time serving), and N6 (agent) are designed and
unbuilt. Because durability here means re-derivation, **N4 is what makes the
weekly wipe survivable.** Until it lands, Silver and Gold hold only what has
streamed in since the last `make up-aws`.

## Prerequisites

- AWS credentials for the sandbox account, through `AWS_PROFILE` or env vars
- `terraform` >= 1.9, `uv`, `docker`, and the `databricks` CLI authenticated to
  the workspace
- Copy each `terraform.tfvars.example` to `terraform.tfvars` and fill it in. For
  `infra/envs/dev`, set `repo_url` and `kafka_client_cidrs` (the Databricks
  workspace NAT Elastic IP; your laptop address is detected automatically).

Every variable, with its default and its effect: [`docs/SETUP.md`](docs/SETUP.md) §5.

## Local development

```bash
make compose-up        # single-broker Kafka on localhost:9092, empty
make check             # ruff + mypy + unit tests
make test-integration  # end to end against the local broker
make compose-down
```

`make compose-up` gives you a broker with no topics and no data. To get data,
run the full local stream. It runs in the foreground, so leave it in its own
terminal:

```bash
make stream-local      # compose Kafka + topics + Binance and Coinbase producers
```

The producers run on the host rather than in the `producers` container. See
Known limitations for why the container cannot reach the broker.

**Notebooks.** [`notebooks/`](notebooks/README.md) is the interactive loop:
stream health and sequence gaps, exploratory analysis over a bounded window, and
a pandas prototype of the Silver and Gold transforms with the PySpark each maps
to. Jupyter and pandas sit in an opt-in dependency group, so they never reach
`make check` or the producer image.

```bash
make stream-local            # in one terminal
make notebook TARGET=local   # in another
```

Against a live MSK deployment, one command refreshes SSO, updates your current
`/32` through Terraform, verifies Kafka access, and launches Jupyter:

```bash
make notebook TARGET=msk
```

**Ad-hoc consumer.** Decodes the same bare Avro the producer writes and computes
a rolling per-instrument VWAP:

```bash
uv run python -m scripts.consume_example \
  --bootstrap "$(terraform -chdir=infra/envs/dev output -raw bootstrap_brokers_public)" \
  --username  "$(terraform -chdir=infra/envs/dev output -raw sasl_username)" \
  --password  "$(terraform -chdir=infra/envs/dev output -raw sasl_password)"
```

Use `--bootstrap localhost:9092` with no credentials against `make compose-up`.

## Kafka topics

| Topic | Partitions | Retention | Populated |
|---|---|---|---|
| `md.trades.v1` | 6 | 24h | yes |
| `md.book.top.v1` | 6 | 6h | no producer code |
| `md.book.depth.v1` | 6 | 2h (off by default) | no producer code |
| `md.bars.v1` | 3 | 48h | no producer code |
| `news.articles.v1` | 3 | 7d | no producer code |
| `ops.metrics.v1` | 1 | 7d | no producer code |

Why these numbers, and what a partition or a retention setting does:
[`docs/KAFKA_EXPLAINED.md`](docs/KAFKA_EXPLAINED.md).

## Known limitations

- **Teardown loses in-flight data by design.** `make down` destroys the brokers,
  and anything not yet read into Bronze is gone. Backfill re-derives it from
  public archives, and the reconciliation job proves it converged. Neither has
  been built yet on the AWS-native side.
- **Binance gets exact-range gap repair; Coinbase gets best-effort.** Coinbase's
  public market-data API has no id-range query, so repair refetches recent
  trades and relies on natural-key dedupe.
- **Equity coverage is IEX-only**, roughly 2% of volume, on Alpaca's free tier.
  Crypto carries the streaming workload.
- **`docker compose --profile live` cannot deliver to the local broker.** The
  Kafka container advertises the host loopback address, which from inside a
  sibling container points at the `producers` container rather than the broker.
  Producers work against real MSK and in the integration tests, which connect
  from the host. Fixing it locally needs a second container-internal listener.
- **DLQ topics are provisioned but never written to.** The `_dlq.*` topics exist
  per the design's dead-letter contract, but no code publishes to them. An
  unparseable frame raises an exception and the WebSocket session reconnects.

The full built-versus-designed gap list is
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §5.
