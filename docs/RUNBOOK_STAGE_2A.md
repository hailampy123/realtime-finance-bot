# Runbook — Stage 2a Bronze + Silver Pipeline

How to reproduce every part of stage 2a from nothing, ad hoc. Design rationale
lives in
[`docs/superpowers/specs/2026-08-12-stage-2a-bronze-silver-pipeline-design.md`](superpowers/specs/2026-08-12-stage-2a-bronze-silver-pipeline-design.md);
this file is only the commands and the order to run them in.

## 1. What stage 2a is

Three tables in Unity Catalog, owned by one Lakeflow Declarative Pipeline:

| Table | What it holds |
|---|---|
| `fdai.market.bronze_trades_stream` | Every Kafka record: decoded Avro, Kafka metadata, and the original bytes. Rejects nothing. |
| `fdai.market.silver_trades` | Deduplicated trade facts, keyed upsert on `(venue, trade_id)`, Change Data Feed on. |
| `fdai.market.silver_trades_quarantine` | Every rejected record, with the reason and the raw bytes. |

**Nothing has read from Kafka yet.** The pipeline is deployable and fully tested
offline, but a live run is blocked on §6.

## 2. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| `uv` | any recent | `brew install uv` |
| Databricks CLI | >= v0.292.0 | verified against v1.11.0 — `brew install databricks/tap/databricks` |
| JDK 17 | any 17.x | `brew install openjdk@17`, **local tests only** |

Homebrew does not link `openjdk@17` onto `PATH`, so `java -version` will still
fail after installing it. That is expected: `make lakehouse-test` sets
`JAVA_HOME` itself. Override with `make lakehouse-test JAVA_HOME_17=/your/jdk`.

A Databricks profile must exist for the target workspace. This project uses
`tw` → `itoc-training-data-ai`. Check with `databricks auth profiles`; every
`make pipeline-*` target takes `DB_PROFILE=<name>` if yours differs.

## 3. Run the test suite from a clean clone

```bash
git clone <repo> && cd finance_data_ai
uv sync --group lakehouse     # pulls pyspark 3.5.3, ~300MB
make lakehouse-test
```

Expect **45 passed**. The first run also downloads `spark-avro_2.12:3.5.3` from
Maven into `~/.ivy2`; every run after that is fully offline.

Without `--group lakehouse` the pyspark-dependent modules skip rather than
error, so `make test` still works — it just covers less.

Everything else:

```bash
make lint          # ruff check + format --check
make typecheck     # mypy, strict
make test          # whole suite
```

`make typecheck` exempts `lakehouse/pipelines/*` — those files import
`pyspark.pipelines` and the `dbutils` global, which exist only on Databricks
Runtime. A test asserts they stay logic-free so the exemption cannot hide a bug.

## 4. Recreate the Databricks objects

All three commands are idempotent enough to re-run; the first two fail harmlessly
if the object already exists.

```bash
databricks catalogs create fdai --comment "finance-data-ai lakehouse" --profile tw
databricks schemas create market fdai --profile tw
databricks secrets create-scope fdai --profile tw
```

Creating a catalog needs `CREATE_CATALOG` on the metastore. Workspace-admin
membership was sufficient here even though the explicit grant list did not name
the user. If it is refused, use a schema inside a catalog you already own and
change `catalog:`/`schema:` in `resources/trades.pipeline.yml` — nothing else in
the design depends on the names.

The pipeline reads three secrets from the `fdai` scope:

```bash
databricks secrets list-secrets fdai --profile tw
# expect: kafka_bootstrap, kafka_username, kafka_password
```

`make up` (`scripts/bootstrap.sh`) publishes these automatically after it builds
MSK. Note it calls the CLI with **no `--profile`**, so the CLI's *default*
profile must point at the intended workspace, or the secrets land somewhere else
and the pipeline authenticates against nothing.

## 5. Validate and deploy

```bash
make pipeline-validate    # databricks bundle validate
make pipeline-deploy      # syncs the repo, creates/updates the pipeline
make pipeline-status      # bundle summary, includes the pipeline URL
```

Deploy syncs the whole repo except `notebooks/`, `graphify-out/`, `infra/`,
`docker/` and `.venv/`. `ingest/schemas/trade.v1.avsc` **must** be synced — the
pipeline reads that exact file at runtime, which is what stops the consumer's
schema drifting from the producer's. Verify:

```bash
databricks workspace list \
  /Workspace/Users/<you>/.bundle/finance-data-ai/dev/files/ingest/schemas --profile tw
```

Trigger a run with `make pipeline-run` (it deploys first — code changes have no
effect until deployed).

Classic compute has no warm pool in this workspace, so the first update can sit
in `WAITING_FOR_RESOURCES` for 10–20 minutes while a cluster is provisioned.
That is normal; do not cancel it. Poll the **update**, not the pipeline:

```bash
databricks pipelines get-update <pipeline-id> <update-id> --profile tw
databricks pipelines list-pipeline-events <pipeline-id> --profile tw
```

When an update fails, the useful text is at `error.exceptions[0].message` — the
top-level `message` only ever says "Update X is FAILED".

## 6. What still blocks a live run

Separate infrastructure work, in this order. Until it is done, the pipeline
deploys and validates but cannot read a single record.

1. **Move MSK to `ap-southeast-2`.** The Databricks metastore is in
   `ap-southeast-2`, but `infra/envs/dev/terraform.tfvars` says `us-east-1`,
   while `terraform.tfvars.example` and the `variables.tf` default say
   `ap-southeast-1`, and the `fdai-sandbox` AWS profile also defaults to
   `ap-southeast-1`. Co-locating Kafka with Databricks removes a ~200 ms
   cross-Pacific round trip and cross-region egress charges on every trade.
2. **Find the workspace NAT egress IP.** It cannot be read from the Databricks
   side, because the workspace VPC lives in a ThoughtWorks-managed AWS account.
   Get it empirically: run a throwaway notebook on a **classic** cluster in this
   workspace and call an IP-echo service (`curl -s https://checkip.amazonaws.com`).
   Whatever it prints is the `/32` MSK will see. It must be a classic cluster —
   serverless egresses from different, rotating addresses.
3. **Allowlist that `/32`.** Put it in `infra/envs/dev/terraform.tfvars` as
   `kafka_client_cidrs = ["<ip>/32"]` — it is currently `[]`, which is why
   Databricks has never reached MSK — then re-run `make up`.

## 7. After the weekly sandbox wipe

The AWS sandbox is destroyed weekly. MSK comes back as a new cluster with a new
topic, so `bronze_trades_stream`'s Kafka checkpoint refers to offsets that no
longer exist. The recovery is:

```bash
make pipeline-refresh-bronze
```

**Never run a whole-pipeline full refresh.** It would also full-refresh
`silver_trades`, destroying accumulated history whose source data the wipe has
already deleted — unrecoverable. Refreshing Bronze alone is safe because
`silver_trades` is a keyed upsert: replaying Bronze re-upserts the same
`(venue, trade_id)` keys and converges instead of duplicating. There is
deliberately no Makefile target for the destructive form.

The `/32` from §6 also changes whenever the workspace NAT EIP changes, and your
own IP is re-detected by `scripts/bootstrap.sh` on every run.

## 8. Verifying it actually worked

Once data flows, run these three. The first two are informational; the third
must return **zero rows**.

```sql
-- 1. Volume by provenance. Stage 2a should show only STREAM.
SELECT source, COUNT(*) AS rows, MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
FROM fdai.market.silver_trades
GROUP BY source;

-- 2. Quarantine rate, and why. Target is < 0.1% of records.
SELECT _quarantine_reason, COUNT(*) AS rows
FROM fdai.market.silver_trades_quarantine
GROUP BY _quarantine_reason
ORDER BY rows DESC;

-- 3. Immutability tripwire -- MUST be empty.
--    Any row means SCD Type 1 has become lossy and the design must move to
--    SCD Type 2. The query is kept in lakehouse/trades/checks.py.
SELECT venue, trade_id,
       COUNT(DISTINCT event_ts_us) AS distinct_event_ts,
       COUNT(DISTINCT price)       AS distinct_price,
       COUNT(DISTINCT size)        AS distinct_size
FROM fdai.market.silver_trades
GROUP BY venue, trade_id
HAVING distinct_event_ts > 1 OR distinct_price > 1 OR distinct_size > 1;
```

A quarantine rate of exactly 0% across a large batch deserves suspicion rather
than satisfaction — it more often means the branch is not wired than that the
data is perfect. Confirm the reason breakdown is empty *and* that
`bronze_trades_stream` and `silver_trades` row counts differ by exactly the
quarantine count.

## 9. Teardown

```bash
databricks bundle destroy -t dev --profile tw
```

This removes the pipeline and the synced files. It does **not** drop
`fdai.market` or its tables — dropping data is deliberately manual:

```sql
DROP TABLE IF EXISTS fdai.market.silver_trades;
DROP TABLE IF EXISTS fdai.market.silver_trades_quarantine;
DROP TABLE IF EXISTS fdai.market.bronze_trades_stream;
```
