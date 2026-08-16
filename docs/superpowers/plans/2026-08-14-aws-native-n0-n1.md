# AWS-Native Workstream — Stages N0 + N1 Run Guide

> **Everything this stage needs already exists in your working tree** — every
> Terraform module, `ingest/core/sinks.py`, the `awsnative/` package and its
> tests, the Dockerfile, the scripts. Nothing has been staged or committed.
> This is a runbook, not a build-it-yourself walkthrough: each task tells you
> which file to open and read, then which commands to run against real AWS
> (or locally) and what to expect back. The AWS interaction and the console
> exploration are the parts that teach you the platform — retyping code
> that's already sitting in the repo would not have.
>
> **Committing is entirely up to you.** Nothing below tells you to commit.
> Commit at whatever granularity and with whatever messages you like, whenever
> you're ready — one commit per task below mirrors the structure if you want
> it, but that's a suggestion, not an instruction. A set of suggested,
> already-written commit messages (one per logical unit, matching the "why"
> explained in each task) is collected at the very end if you want a
> starting point.

**Goal:** Live Binance and Coinbase trades flowing into S3 as Parquet, queryable in Athena,
with the whole stack reproducible from an empty AWS account by `make up-aws`.

**Architecture:** Reused `ingest/` connectors run as one ECS Fargate task writing JSON to
Kinesis Data Streams. Firehose converts JSON→Parquet and lands it in S3 partitioned by
arrival date. A Glue table with partition projection makes it queryable in Athena with no
crawler.

**Tech Stack:** Terraform ~> 5.0 AWS provider, Python 3.12 + boto3, ECS Fargate, ECR,
Kinesis Data Streams, Kinesis Data Firehose, S3, Glue Data Catalog, Athena.

**Spec:** [`2026-08-14-aws-native-workstream-design.md`](../specs/2026-08-14-aws-native-workstream-design.md)
— read §3, §4 and §5.1 before starting. This guide covers stages N0 and N1 of §12.

## Global Constraints

These already hold across everything that's been placed in the repo — useful to know as you
read the code, and binding on anything you add yourself later:

- Terraform `required_version >= 1.9.0`, AWS provider `~> 5.0` — matches the existing MSK
  root at `infra/envs/dev`.
- Project prefix is `fdai`; every AWS-native resource is named `fdai-native-*` so the two
  workstreams never collide on a name.
- `default_tags` on the provider is `{ Project, ManagedBy = "terraform", Ephemeral = "true" }`
  — the wipe-safety convention from `infra/envs/dev/main.tf`.
- Terraform state goes in the **existing** `fdai-tfstate-<account-id>` bucket under key
  `native/terraform.tfstate`, locked by the existing `fdai-tflock` DynamoDB table. Never
  reuse the `dev/terraform.tfstate` key — that is the MSK stack's state.
- `ingest/` has no AWS dependency. All boto3 code lives under `awsnative/`.
- `ingest/schemas/trade.v1.avsc` stays the single source of truth for the record shape.
- `make check` (lint + mypy + unit tests) is green right now. It must stay green.
- Nothing durable lives in this account. Every resource is destroyable by
  `make down-aws` and recreatable by `make up-aws`.

## What's already in your repo

**Python — the AWS-specific tree:**

| File | Responsibility |
|---|---|
| `ingest/core/sinks.py` | The `Sink` protocol, extracted from `runner.ProducerLike`. Transport-agnostic. |
| `awsnative/__init__.py` | Package marker. |
| `awsnative/encode.py` | `Trade` → JSON bytes, carrying `schema_version`. The only place the wire format is decided. |
| `awsnative/sink.py` | `KinesisSink` — batching, partition keying, partial-failure retry. |
| `awsnative/settings.py` | `NativeSettings` — env config for the AWS producer (`FDAI_NATIVE_` prefix). |
| `awsnative/cli.py` | Entrypoint: connectors + `KinesisSink` + `IngestRunner`. Mirrors `ingest/cli.py`. |

**Tests:**

| File | Responsibility |
|---|---|
| `tests/awsnative/test_encode.py` | JSON↔`.avsc` contract (the drift tripwire). |
| `tests/awsnative/test_sink.py` | Batching, flush triggers, partial-failure retry, backoff. |

**Terraform — one module per concern:**

| Path | Responsibility |
|---|---|
| `infra/modules/native_network/` | VPC, IGW, 2 public subnets, one egress-only SG. |
| `infra/modules/native_lakehouse/` | S3 lake bucket, Glue database, Bronze Glue table, Athena workgroup. |
| `infra/modules/native_stream/` | Kinesis stream, Firehose delivery stream, Firehose IAM role. |
| `infra/modules/native_producer/` | ECR repo, ECS cluster, task definition, task + execution roles, service. |
| `infra/envs/native/` | Root module wiring all four together. Own backend key. |

**Scripts and Docker:**

| Path | Responsibility |
|---|---|
| `scripts/native_preflight.sh` | Probes every AWS service this stack needs. Answers spec §11 A5. |
| `scripts/native_up.sh` | Ordered bring-up: preflight → apply → image push → force new deployment. |
| `docker/Dockerfile.awsnative` | Producer image with the `awsnative` dependency group. |
| `awsnative/sql/verify_bronze.sql` | The stage N1 acceptance queries. |

**Why `native_network` is a new module rather than reusing `infra/modules/network`:** the
existing module requires a `kafka_client_cidrs` variable and creates an MSK security group
with Kafka ports. Reusing it would mean passing a Kafka-shaped input this stack has no
opinion about, and carrying a security group nothing attaches to. A 50-line module with one
job is cleaner than a shared module with two personalities.

**Where things stand right now:** `make check` has been run — 209 passed, 7 skipped, lint
clean, `ruff format` clean, mypy clean across `ingest devlab lakehouse awsnative` (35 source
files). One unrelated pre-existing failure in
`tests/devlab/test_stream_product_research_notebook.py` predates this work — it's checking a
notebook you already had modified before this session, nothing to do with the AWS-native
code. Nothing Terraform-shaped has been touched: no `init`, no `plan`, no `apply`, no
`docker build`. Those are all still yours to run, starting below.

**A note on `-target` in Tasks 2–3 and 8–11.** Because every module already exists fully
written, a plain `terraform apply` right now would create the *entire* stack in one shot —
you'd lose the "apply one piece, go look at it, understand it, apply the next piece"
progression that building it up by hand would have given you. `-target=module.X` gets that
progression back: it tells Terraform to create only that module's resources (plus anything
it depends on) and leave the rest of the configuration alone for now. **This is a deliberate
teaching device, not how you'd normally run Terraform** — day to day, and in `make up-aws`,
it's one plain `terraform apply` for the whole configuration. You'll do exactly that,
untargeted, once near the end of Task 11 and again in Task 13, to prove the whole graph
agrees with itself.

---

## Task 1: Preflight — prove the account can build this stack

**Concepts.** Before touching infrastructure, find out what the account permits. A **Service
Control Policy (SCP)** is an organisation-level rule that can deny AWS actions no matter what
your IAM permissions say. The MSK workstream was built around the belief that
`iam:CreateRole` was denied here; that turned out to be wrong, and this whole stack depends
on it being allowed. Verify rather than assume: a failed probe now costs a minute, discovering
it mid-stack costs an afternoon.

**Read:** `scripts/native_preflight.sh` — read-only against every service this stack needs,
except one IAM create-then-delete round trip. Listing roles proves nothing about whether
*creating* one is permitted; calling it is the only real test.

- [ ] **Step 1: Confirm which account and region you're in**

```bash
aws sso login --profile <configured-profile> # eg. fdai-sandbox
export AWS_PROFILE=<configured-profile>
aws sts get-caller-identity
aws configure get region
```

Expected: an `Account` number and a region string. Write both down — the account number
appears in the state bucket name, and everything must live in **one** region. If the region is
empty, set it: `export AWS_REGION=ap-southeast-2` (or whichever you use), and add it to your
shell profile.

- [ ] **Step 2: Run the preflight script**

```bash
./scripts/native_preflight.sh
```

Expected: every line `ok`, exit 0.

If `iam:CreateRole` fails, **stop** — this stack is not buildable and you should re-read spec
§11 A5. If a single other service fails (say `stepfunctions`), that service's later stage is
blocked but N0/N1 may still proceed: N1 needs only kinesis, firehose, s3, glue, athena, ecr,
ecs, ec2, logs, and iam.

---

## Task 2: Terraform root and the shared state backend

**Concepts.** **Terraform** describes infrastructure as files, then makes reality match them.
Its **state file** records what it has created, so it knows what to change or destroy next
time. State lives in S3 (shared, durable) with a **DynamoDB lock table** so two runs can't
corrupt it. You already have both from the MSK workstream — this stack reuses them under a
different **key** (a path inside the bucket), which is what keeps the two stacks
independently destroyable. A **root module** is the directory you run `terraform apply` in; it
wires together **child modules**, which are reusable folders of resources.

**Read:** `infra/envs/native/main.tf` and `outputs.tf` — every module block and every output
this stack exposes, all in one place. `versions.tf` pins the provider; `variables.tf` is the
root's inputs; `backend.tf.tftpl` is a template (real values get substituted into a
git-ignored `backend.tf` in the next step).

- [ ] **Step 1: Copy the tfvars example and render the backend**

```bash
cd infra/envs/native
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set your real region and email
ACCT=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)
sed -e "s/\${state_bucket}/fdai-tfstate-$ACCT/" \
    -e "s/\${region}/$REGION/" \
    -e "s/\${lock_table}/fdai-tflock/" \
    backend.tf.tftpl > backend.tf
terraform init
cd -
```

Expected: `Terraform has been successfully initialized!`

If it fails with `NoSuchBucket`, the MSK bootstrap has not run in this account yet. Run
`terraform -chdir=infra/bootstrap apply` first — that creates the shared state bucket and
lock table.

- [ ] **Step 2: Apply just the budget, using `-target`**

```bash
terraform -chdir=infra/envs/native plan -target=aws_budgets_budget.monthly
```

Expected: `Plan: 1 to add, 0 to change, 0 to destroy.`

```bash
terraform -chdir=infra/envs/native apply -target=aws_budgets_budget.monthly
```

Expected: `Apply complete! Resources: 1 added.`

- [ ] **Step 3: Look at what you made**

```bash
aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --query 'Budgets[?BudgetName==`fdai-native-monthly`]'
aws s3 ls "s3://fdai-tfstate-$(aws sts get-caller-identity --query Account --output text)/native/"
```

Expected: the budget JSON, and `terraform.tfstate` under the `native/` prefix — proving the
two stacks' states are separate objects in one bucket.

---

## Task 3: The network module

**Concepts.** A **VPC** is a private network inside AWS; every VPC has a private IP range
(**CIDR**, e.g. `10.43.0.0/16`). A **subnet** is a slice of that range pinned to one
**availability zone** (a physically separate datacentre). A subnet is *public* if its route
table sends internet-bound traffic to an **Internet Gateway (IGW)**. A **security group** is a
stateful firewall attached to a resource.

The producer needs outbound WSS to Binance and Coinbase and nothing inbound. Two ways to give
a container outbound internet: a **NAT Gateway** (container sits in a private subnet, ~$32/mo)
or a **public subnet with a public IP** (free). This stack takes the second — a NAT gateway
would cost more than the entire rest of the platform to buy privacy this workload doesn't need.
Two AZs, not one, because ECS needs somewhere to place a task if one AZ is unavailable.

**Read:** `infra/modules/native_network/main.tf` — VPC, IGW, subnets, route table, and the
one egress-only security group. There is no inbound rule to review because there is no
inbound rule.

**Interfaces:** `module.network.public_subnet_ids` (list), `module.network.egress_security_group_id`,
`module.network.vpc_id` — these feed `module.producer` in Task 10/11.

- [ ] **Step 1: Apply just this module**

```bash
terraform -chdir=infra/envs/native apply -target=module.network
```

Expected: ~8 resources added (VPC, IGW, 2 subnets, route table, 2 associations, SG).

- [ ] **Step 2: Look at what you made — in the console this time**

Open the VPC console, find `fdai-native-vpc`, and click through: **Subnets** (two, different
AZs), **Route tables** → the public one → **Routes** tab (you should see `0.0.0.0/0` → your
IGW; that single row is the entire difference between a public and a private subnet), and
**Security groups** → `fdai-native-egress` → **Inbound rules** (empty).

Then confirm the same from the CLI, so you can see the console and the API agree:

```bash
VPC=$(terraform -chdir=infra/envs/native output -raw vpc_id)
aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC" \
  --query 'Subnets[].{az:AvailabilityZone,cidr:CidrBlock,publicIp:MapPublicIpOnLaunch}'
```

Expected: two subnets, two different AZs, `publicIp: true` on both.

---

## Task 4: The `Sink` protocol

**Concepts.** A Python **Protocol** is a structural interface: any class with matching method
signatures satisfies it, with no inheritance required. `IngestRunner` already depended on one
(`ProducerLike`, formerly in `ingest/runner.py`) rather than on Kafka, which is why this
workstream needed no runner changes beyond moving that definition somewhere both
implementations can import it from.

**Read:** `ingest/core/sinks.py` — the `Sink` protocol (`produce`, `poll`, `flush`) and its
docstring, which names both implementations. Then `ingest/runner.py` — note the import
`from ingest.core.sinks import Sink` and the constructor parameter `producer: Sink`; the old
`ProducerLike` class is gone from this file entirely.

- [ ] **Step 1: Confirm the old name is completely gone**

```bash
grep -rn "ProducerLike" --include="*.py" .
```

Expected: no output. `ProducerLike` had no call sites outside `runner.py`, so removing it
changed no behaviour anywhere else in the codebase.

- [ ] **Step 2: Run the full check**

```bash
make check
```

Expected: green — lint clean, mypy clean, all tests passing (aside from the one pre-existing,
unrelated notebook failure noted above).

---

## Task 5: JSON encoder and the schema-drift tripwire

**Concepts.** Kafka records carry **headers** (out-of-band key/value metadata); Kinesis
records do **not**. The Kafka path puts `schema_version`, `venue`, `is_backfill`, and `source`
in headers — three of those are already Avro fields, so only `schema_version` needs a new
home inside the record body.

Since the envelope has to change anyway, the AWS path sends **JSON** rather than Avro. That
removes a whole component: Firehose converts JSON→Parquet natively against a Glue table
schema, so no Lambda sits in the hot path decoding Avro. The cost is a drift risk — two
independent definitions of the record shape. The test below is what removes it: the JSON is
validated against `trade.v1.avsc` itself, so a field added on one side and not the other
fails CI rather than a 3 a.m. Firehose delivery.

**Read:** `awsnative/encode.py` — `trade_to_dict` and `encode_trade`, both short. Then
`tests/awsnative/test_encode.py` — five tests, the first parametrised four ways, checking the
schema agreement, the field-name agreement, where `schema_version` lives, the exact byte
shape Firehose expects, and enum serialisation.

- [ ] **Step 1: Run the tests**

```bash
uv run --group awsnative pytest tests/awsnative/test_encode.py -v
```

Expected: 8 passed (5 test functions, one parametrised 4 ways).

- [ ] **Step 2: Prove the tripwire actually trips**

This is the important step. A guard you haven't seen fail is an assumption. Temporarily break
the encoder, watch the right test fail, then put it back.

```bash
python - <<'EOF'
import pathlib
p = pathlib.Path("awsnative/encode.py")
s = p.read_text()
p.write_text(s.replace(
    '    record["schema_version"] = TRADE_SCHEMA_VERSION',
    '    record["schema_version"] = TRADE_SCHEMA_VERSION\n    record["bogus_field"] = 1'))
EOF
uv run --group awsnative pytest tests/awsnative/test_encode.py -q
```

Expected: **FAIL** — `test_encoder_and_avro_schema_agree_on_field_names` reports the extra
field. Now revert:

```bash
git checkout -- awsnative/encode.py
uv run --group awsnative pytest tests/awsnative/test_encode.py -q
```

Expected: back to 8 passed. (`git checkout --` here only discards the temporary edit you just
made in this step — it does not touch anything else, and nothing has been staged, so this is
safe.)

- [ ] **Step 3: Confirm the default test run still works without the AWS group**

```bash
make check
```

Expected: green, with the encode tests included. `encode.py` imports nothing from `boto3`, so
plain `make test` (no `--group awsnative`) already covers it.

---

## Task 6: The Kinesis sink

**Concepts.** **Kinesis Data Streams** is a durable, ordered log — Kafka's closest AWS
analogue. Records are grouped into **shards**; a record's **partition key** decides its shard,
so all records with the same key stay ordered relative to each other. `KinesisSink` reuses
`Trade.kafka_key()` (`venue|symbol`) so one instrument on one venue keeps its ordering, the
same property the Kafka key bought. The stream itself (Task 9) is **on-demand**, so AWS
manages shard count.

`put_records` sends up to 500 records per call. The gotcha that catches everyone: **it can
partially fail.** You get HTTP 200 with a `FailedRecordCount` and per-record error codes, and
the failed records are simply gone unless you resend them. Silently dropping them here would
break the one property the spec says this system must not have — silent trade loss. So the
sink retries only the failed subset, with exponential backoff, and blocks rather than drops
when it can't keep up. Blocking propagates as queue backpressure, which
`BoundedTopicQueue`'s BLOCK policy turns into a *detectable, REST-repairable gap*.

**Read:** `awsnative/sink.py` — start with the class docstring, then `produce`/`poll`/`flush`
(the `Sink` protocol surface), then `_send_with_retry` (the partial-failure handling — this is
the part worth understanding line by line). Note `_assert_protocol` at the bottom: an
otherwise-dead function that exists purely so mypy checks `KinesisSink` against `Sink`
structurally, since a signature mismatch is only ever caught where the two actually meet.

**Interfaces:** `KinesisSink(stream_name, *, client=None, max_batch=500, max_bytes=4_500_000,
max_attempts=8, sleep=time.sleep)`, implementing `Sink`.

- [ ] **Step 1: Run the tests**

```bash
uv run --group awsnative pytest tests/awsnative/test_sink.py -v
```

Expected: 10 passed — covering buffering, flush, partition keying, the JSON payload shape,
batch-size triggering, partial-failure retry (only the failed record is resent), growing
backoff, raise-rather-than-drop on exhausted retries, and the byte-size cap.

- [ ] **Step 2: Verify the protocol check actually does something**

```bash
make typecheck
```

Expected: `Success: no issues found`. If you want to see `_assert_protocol` actually catch
something, temporarily rename `flush` to `flsh` in `awsnative/sink.py`, re-run
`make typecheck`, watch mypy complain that `KinesisSink` no longer satisfies `Sink`, then
revert with `git checkout -- awsnative/sink.py`.

- [ ] **Step 3: Run the whole check**

```bash
make check
```

Expected: green.

---

## Task 7: The producer entrypoint

**Concepts.** Nothing new in AWS terms — this is the composition root for the AWS path,
mirroring `ingest/cli.py`. It's a separate module rather than a flag on the existing CLI so
that the Kafka path keeps working with no AWS config present, and so the container image can
select a transport by choosing a command.

**Read:** `awsnative/settings.py` (its own `FDAI_NATIVE_` env prefix, so neither path can be
started with the other's config by accident) and `awsnative/cli.py` (identical shape to
`ingest/cli.py`, but wired to `KinesisSink` — one sink per venue, so a throttled retry on one
venue's batch never stalls the other's drain task).

- [ ] **Step 1: Verify it imports and resolves settings with no AWS credentials**

```bash
uv run --group awsnative python -c "
import awsnative.cli as c
from awsnative.settings import NativeSettings
print('stream:', NativeSettings().stream_name)
print('venues:', NativeSettings().venues)
print('connectors:', sorted(c.CONNECTORS))
"
```

Expected:

```
stream: fdai-native-md-trades-v1
venues: ['binance', 'coinbase']
connectors: ['binance', 'coinbase']
```

- [ ] **Step 2: Run the check**

```bash
make check
```

Expected: green.

---

## Task 8: The lakehouse module — S3, Glue, and the Bronze table

**Concepts.** Three services, one job each:

- **S3** stores the files. A **bucket** is a namespace; a **key** is a path within it.
- **Glue Data Catalog** is a metadata database: it records that a set of S3 files *is* a
  table with named, typed columns. It stores no data. Athena and Firehose both read it.
- **Athena** runs SQL over S3 by asking Glue what the files mean. It is serverless and
  charges per **byte scanned** ($5/TB), which is why partitioning matters — a query that can
  skip a partition doesn't pay for it. A **workgroup** groups queries and sets where results
  go.

**Partitions** are directories named `key=value` (e.g. `ingest_date=2026-08-14/`). Normally
Athena has to be *told* each partition exists, usually by running a **crawler**. **Partition
projection** replaces that: you declare the *pattern* (a date, in this range, one day apart)
and Athena computes partition locations arithmetically. No crawler, no scheduling, and no
window where a partition exists in S3 but not in the catalog.

The Glue table is created **before** Firehose (Task 9), because Firehose reads this table's
schema to know how to write Parquet.

**Read:** `infra/modules/native_lakehouse/main.tf` — the bucket (with `force_destroy = true`,
since this holds only re-derivable data), the lifecycle rules that expire Athena results and
Firehose error output, the Glue database and table (note the `projection.*` parameters and
the `$${ingest_date}` escape — `$${}` is how you tell Terraform "leave this literal for
Athena, don't interpolate it yourself"), and the Athena workgroup.

**Interfaces:** `module.lakehouse.{bucket_arn, bucket_name, glue_database_name,
bronze_table_name, bronze_prefix, athena_workgroup_name}` — consumed by `module.stream`
(Task 9).

- [ ] **Step 1: Apply just this module**

```bash
terraform -chdir=infra/envs/native apply -target=module.lakehouse
```

Expected: ~7 resources added.

- [ ] **Step 2: Query the empty table — proving the catalog works before any data exists**

This is the single most useful check in the task: it separates "is the table defined
correctly" from "is the data arriving", which are otherwise diagnosed together and badly.

```bash
WG=$(terraform -chdir=infra/envs/native output -raw athena_workgroup)
DB=$(terraform -chdir=infra/envs/native output -raw glue_database)

QID=$(aws athena start-query-execution \
  --work-group "$WG" \
  --query-execution-context "Database=$DB" \
  --query-string "SELECT count(*) AS rows FROM bronze_trades_stream" \
  --query QueryExecutionId --output text)

sleep 6
aws athena get-query-execution --query-execution-id "$QID" \
  --query 'QueryExecution.Status.[State,StateChangeReason]' --output text
aws athena get-query-results --query-execution-id "$QID" \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
```

Expected: `SUCCEEDED` and `0`.

`SUCCEEDED` with `0` means the table, the SerDe, and partition projection are all correct and
the prefix is simply empty. If you instead see `FAILED`, read the `StateChangeReason`:
`HIVE_METASTORE_ERROR` usually means a malformed projection property, and a message about
`storage.location.template` means the `$${ingest_date}` escape did not survive into the
catalog.

- [ ] **Step 3: Look at the table definition in the console**

Open the Athena console → **Query editor** → set Workgroup to `fdai-native` and Database to
`fdai_native`. Expand `bronze_trades_stream` in the left pane and confirm the 13 columns plus
the `ingest_date` partition key. Then Glue console → **Tables** → `bronze_trades_stream` →
**Table properties**, and read the `projection.*` keys — this is where you can see that no
partition is *registered*, only *described*.

---

## Task 9: The stream module — Kinesis and Firehose

**Concepts.** **Kinesis Data Streams** is the durable buffer (Task 6's Concepts covers
partition keys and on-demand mode). **Kinesis Data Firehose** is a managed delivery pipeline:
point it at a source, and it batches records, optionally transforms them, and writes them to a
destination. Here it reads the stream, converts JSON→Parquet using the Glue table's schema,
and writes to S3.

Two Firehose details matter:

- **Buffering hints** — it writes when *either* the size (128 MB) or the interval (120 s)
  threshold is hit, whichever comes first. Bigger buffers mean fewer, larger files (cheaper to
  query) but staler data. 120 s puts producer→Bronze lag around 2 minutes.
- **The custom prefix** — `ingest_date=!{timestamp:yyyy-MM-dd}/` makes Firehose write
  Hive-style partition directories. This is the *time-based* prefix feature and is free;
  don't confuse it with **dynamic partitioning**, which keys off record *content* and is
  billed per GB.

**IAM** needs explaining because it's where most people get stuck. A **role** is a set of
permissions something can *assume*. It has two halves: a **trust policy** (who may assume
it — here, `firehose.amazonaws.com`) and **permission policies** (what it may then do). AWS
services act on your behalf by assuming a role you create for them.

**Read:** `infra/modules/native_stream/main.tf` — top to bottom it's: the Kinesis stream
(on-demand, KMS-encrypted), the Firehose trust policy and permission policy (stream read, KMS
decrypt, S3 write, Glue schema read, log write), the log group, then the Firehose delivery
stream itself (`prefix`, the format-conversion block with the JSON deserializer and Parquet
serializer, and the schema lookup pointing at the Bronze table).

**Interfaces:** `module.stream.{stream_name, stream_arn, firehose_log_group}` — consumed by
`module.producer` (Task 10/11).

- [ ] **Step 1: Apply just this module**

```bash
terraform -chdir=infra/envs/native apply -target=module.stream
```

Expected: ~7 resources added.

- [ ] **Step 2: Test the whole pipe by hand, before any container exists**

This is the highest-value verification in the whole guide. Put one record in with the CLI and
watch it come out the other end as Parquet — that tests the stream, the Firehose role, the
JSON deserializer, the Parquet serializer, the Glue schema lookup, and the S3 prefix, without
any producer code involved. If this works, a later failure is your producer; if it doesn't,
it's never your producer.

Build the record with the real encoder rather than hand-typed JSON, so this tests the exact
bytes your producer will send:

```bash
STREAM=$(terraform -chdir=infra/envs/native output -raw stream_name)
BUCKET=$(terraform -chdir=infra/envs/native output -raw lake_bucket)

RECORD=$(uv run --group awsnative python - <<'EOF'
from awsnative.encode import encode_trade
from ingest.core.models import Side, Source, Trade

print(encode_trade(Trade(
    venue="binance", venue_symbol="BTCUSDT", instrument_id="BTC-USD",
    trade_id="manual-1", event_ts_us=1_754_000_000_000_000,
    ingest_ts_us=1_754_000_000_100_000, price="61234.56", size="0.0123",
    side=Side.BUY, sequence=1, is_backfill=False, source=Source.STREAM,
)).decode())
EOF
)
echo "$RECORD"

aws kinesis put-record \
  --stream-name "$STREAM" \
  --partition-key "binance|BTCUSDT" \
  --data "$RECORD" \
  --cli-binary-format raw-in-base64-out
```

Expected: the JSON echoed, then a `ShardId` and `SequenceNumber`.

Note `--cli-binary-format raw-in-base64-out`. Without it, AWS CLI v2 base64-encodes your
`--data` again and Firehose receives gibberish it cannot parse as JSON — one of the most
common ways this step "silently" fails.

- [ ] **Step 3: Wait for the buffer, then look for the Parquet file**

Firehose will not write until the 120-second interval elapses.

```bash
sleep 140
aws s3 ls "s3://$BUCKET/bronze_trades_stream/" --recursive
```

Expected: one object under `bronze_trades_stream/ingest_date=<today>/`, name ending in
`.parquet`, non-zero size.

If nothing appeared after ~3 minutes, check the error prefix and the logs:

```bash
aws s3 ls "s3://$BUCKET/_errors/" --recursive
aws logs tail "$(terraform -chdir=infra/envs/native output -raw firehose_log_group)" --since 10m
```

An object under `_errors/format-conversion-failed/` means the JSON did not match the Glue
schema — read the error object, it names the offending field. An IAM message in the logs
means a missing permission in the Firehose policy.

- [ ] **Step 4: Read it back through Athena**

```bash
WG=$(terraform -chdir=infra/envs/native output -raw athena_workgroup)
DB=$(terraform -chdir=infra/envs/native output -raw glue_database)

QID=$(aws athena start-query-execution --work-group "$WG" \
  --query-execution-context "Database=$DB" \
  --query-string 'SELECT trade_id, venue, price, "size", side, ingest_date FROM bronze_trades_stream' \
  --query QueryExecutionId --output text)
sleep 8
aws athena get-query-results --query-execution-id "$QID" \
  --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output table
```

Expected: a header row plus one data row containing `manual-1`, `binance`, `61234.56`, `BUY`,
and today's date.

`size` is quoted in the SQL because it reads more safely as an identifier that way; the same
habit avoids surprises with any column that shadows a function name.

**You have now proven the entire ingest path works.** Everything after this is running your
own code through a pipe you know is good.

---

## Task 10/11: Build the producer image and run it on Fargate

**Concepts.** **ECR** (Elastic Container Registry) is a private Docker registry. Fargate can
only run images it can pull, so the image has to live somewhere AWS-reachable.

The trap, and it will get you: your Mac is **arm64** (Apple Silicon), and Fargate's default
CPU architecture is **x86_64**. An image built without `--platform linux/amd64` pushes fine,
then fails at runtime with `exec format error` — a message that tells you nothing about
architecture. Build for the target explicitly.

**ECS** (Elastic Container Service) is an orchestrator. **Fargate** is its serverless mode:
you describe a container, AWS finds the compute. Three pieces:

- **Cluster** — a namespace. Free; it holds no capacity in Fargate mode.
- **Task definition** — the spec of what to run: image, CPU, memory, env vars, logging, and
  two roles.
- **Service** — keeps N copies of a task definition running, restarting them if they die.

The **two roles** confuse everyone, so: the **execution role** is used by the ECS *agent*
before your code starts — pull the image from ECR, create the log stream. The **task role** is
used by *your code* while it runs — here, `kinesis:PutRecords`. Different actors, different
lifetimes, different permissions. boto3 inside the container finds the task role
automatically via a link-local credential endpoint, which is why no key is ever configured.

`desired_count` is 1 deliberately: two tasks would each open their own Binance and Coinbase
WebSocket connections and publish every trade twice.

**Read:** `docker/Dockerfile.awsnative` (no credentials baked in — the task role supplies
them at runtime), then `infra/modules/native_producer/main.tf` top to bottom: ECR repo +
lifecycle policy, the two IAM roles (`ecs_trust`/`execution` and `task`/`task_permissions` —
notice the task policy is exactly two actions on exactly one resource), the log group, the
cluster, the task definition (note `runtime_platform.cpu_architecture = "X86_64"` — this must
agree with how you build the image), and the service (`assign_public_ip = true`,
`deployment_minimum_healthy_percent = 0` — a deliberate stop-then-start rather than rolling
replacement).

**Interfaces:** `module.producer.{ecr_repository_url, cluster_name, service_name, log_group}`.

- [ ] **Step 1: Apply the whole producer module**

Because ECR and ECS are defined together in one file, targeting `module.producer` creates
both at once — including the ECS service, before any image exists in ECR.

```bash
terraform -chdir=infra/envs/native apply -target=module.producer
```

Expected: ~11 resources added. The service will come up but the task will fail to start —
that's expected and is checked for in Step 4 below; don't troubleshoot it yet.

- [ ] **Step 2: Log in to ECR, build for the right architecture, and push**

`--platform linux/amd64` is not optional on Apple Silicon. Without it the push succeeds and
the task dies with `exec format error`, which says nothing about architecture.

```bash
REGION=$(terraform -chdir=infra/envs/native output -raw region)
REPO=$(terraform -chdir=infra/envs/native output -raw ecr_repository_url)
IMAGE="${REPO}:latest"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"

docker build --platform linux/amd64 \
  -f docker/Dockerfile.awsnative \
  -t "$IMAGE" .

docker push "$IMAGE"
```

Expected: `login succeeded`, a completed build, and a push ending in a `digest: sha256:...`
line.

- [ ] **Step 3: Verify the pushed image's architecture, then smoke-test it locally**

```bash
docker image inspect "${REPO}:latest" --format '{{.Architecture}}'
```

Expected: **`amd64`**. If it says `arm64`, rebuild with `--platform`.

```bash
docker run --rm --platform linux/amd64 "${REPO}:latest" \
  python -c "import awsnative.cli, boto3; print('imports ok', boto3.__version__)"
```

Expected: `imports ok 1.3x.x`. This catches a missing dependency in seconds, where the same
mistake in Fargate costs a log-hunting round trip.

- [ ] **Step 4: Force a new deployment so the service picks up the image**

```bash
CLUSTER=$(terraform -chdir=infra/envs/native output -raw ecs_cluster)
SERVICE=$(terraform -chdir=infra/envs/native output -raw ecs_service)
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" --force-new-deployment
aws ecs wait services-stable --cluster "$CLUSTER" --service "$SERVICE"
aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
  --query 'services[0].{desired:desiredCount,running:runningCount,pending:pendingCount}'
```

Expected: `running: 1, pending: 0`.

If `running` stays 0, the reason is almost always in the service events:

```bash
aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
  --query 'services[0].events[0:5].message' --output text
```

- [ ] **Step 5: Read the producer's logs**

```bash
aws logs tail "$(terraform -chdir=infra/envs/native output -raw producer_log_group)" --since 5m --follow
```

Expected: JSON lines including `starting_venue` for both `binance` and `coinbase` with a
`stream` field, then quiet (the runner logs on events, not per trade). Press Ctrl-C to stop.

Common failures and what they mean:

| Log or event | Cause |
|---|---|
| `exec format error` | arm64 image. Rebuild with `--platform linux/amd64` (Step 2 above). |
| `AccessDeniedException ... kinesis:PutRecords` | Task role policy missing or wrong stream ARN. |
| `CannotPullContainerError` (persisting after Step 4) | Execution role or no route to ECR — check `assign_public_ip`. |
| `ValidationError: no venues to run` | `config/` did not make it into the image. |
| Nothing at all in the log group | Execution role can't create the stream; check the managed policy attachment. |

- [ ] **Step 6: Confirm records are actually arriving in Kinesis**

Logs proving the process started are not the same as data flowing. Check the stream's own
metrics:

```bash
STREAM=$(terraform -chdir=infra/envs/native output -raw stream_name)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis --metric-name IncomingRecords \
  --dimensions "Name=StreamName,Value=$STREAM" \
  --start-time "$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 60 --statistics Sum \
  --query 'sort_by(Datapoints,&Timestamp)[].{t:Timestamp,records:Sum}' --output table
```

Expected: rising `records` per minute — a few hundred to a few thousand depending on market
activity.

- [ ] **Step 7: Reconcile — one plain `terraform apply`, no target**

Every module has now been applied piece by piece via `-target`. Run one untargeted apply to
confirm the whole configuration agrees with what's actually running — this is also exactly
how you'll run it day to day from here on.

```bash
terraform -chdir=infra/envs/native apply
```

Expected: `Apply complete! Resources: 0 added, 0 changed, 0 destroyed.` (or a small number of
`0 changed` no-op refreshes — not new resources). If Terraform wants to *create* something
here, one of the `-target` applies above was skipped; if it wants to *change or destroy*
something, look closely at what before proceeding.

---

## Task 12: End-to-end verification in Athena

**Concepts.** Nothing new — this is the gate. Everything so far has been verified component
by component; this proves the composed system produces *correct* data, not merely data.

**Read:** `awsnative/sql/verify_bronze.sql` — three queries checking three different failure
modes: rows from both venues (a missing venue means a connector failed silently), no
systematic nulls (Firehose's OpenX deserializer silently writes NULL for a JSON key that does
not match a Glue column — the failure the Task 5 contract test *cannot* catch, because the
Glue table is a third party to that agreement), and sane values (a working pipe carrying
wrong numbers).

- [ ] **Step 1: Let it run for at least five minutes**

Firehose buffers for 120 s, so anything less than ~3 minutes tells you nothing. Five gives you
two or three files and enough rows for the null checks to mean something.

- [ ] **Step 2: Run query 1 — rows from both venues**

Use the Athena console for this (Query editor, workgroup `fdai-native`, database
`fdai_native`) — reading a result table beats parsing CLI JSON, and you should see the
console's "Data scanned" figure, which is what you're billed on.

Expected: two rows, `binance` and `coinbase`, both with non-zero `rows`, `distinct_trades`
close to `rows`, and `last_event` within the last couple of minutes.

If `coinbase` is missing, check the producer log for that venue — the Coinbase connector needs
a subscribe payload accepted before it emits anything.

- [ ] **Step 3: Run query 2 — no systematic nulls**

Expected: `rows` > 0 and **every** `null_*` column exactly `0`.

Any non-zero null count except `null_sequence` (legitimately nullable for Coinbase) means the
Glue column name and the JSON key disagree. Compare
`infra/modules/native_lakehouse/main.tf`'s `columns` blocks against `awsnative/encode.py`.

- [ ] **Step 4: Run query 3 — values are sane**

Expected: every column `0`.

---

## Task 13: `make up-aws` / `make down-aws`

**Concepts.** The reproducibility contract from the parent spec: *any manual console step is
a bug.* Everything you've run by hand above is now collapsed into one ordered command,
already written for you, so the next weekly rebuild is a single invocation.

Ordering matters and is not obvious. Terraform can create the ECS service before an image
exists in ECR (you saw this in Task 10/11 Step 1); the service then sits retrying
`CannotPullContainerError` until an image appears. So `scripts/native_up.sh` applies
infrastructure, pushes the image, *then* forces the service to redeploy.

**Read:** `scripts/native_up.sh` — the whole flow in one file: preflight → render backend →
`terraform apply` (untargeted, the whole configuration) → build/push → force-new-deployment →
print where to look next. Then the `up-aws`/`down-aws`/`rebuild-aws`/`validate-aws` targets in
`Makefile`.

- [ ] **Step 1: Validate the Terraform offline**

```bash
make validate-aws
```

Expected: `Success! The configuration is valid.` `tflint` and `checkov` are optional — if
they're not installed the target says so and carries on rather than failing. Install them
(`brew install tflint`, `uv tool install checkov`) for the extra pass; `checkov` will flag the
public-subnet task and the unrestricted egress SG, both of which are deliberate and
documented in spec §4.4.

- [ ] **Step 2: Prove the reproducibility contract — destroy and rebuild from empty**

This is the acceptance test for the whole stage. Anything that only exists because you ran a
command by hand and Terraform doesn't know about it disappears here and does not come back.

```bash
make down-aws
```

Expected: `Destroy complete! Resources: N destroyed.` with no errors.

```bash
aws kinesis list-streams --query 'StreamNames[?contains(@,`native`)]'
aws ecs list-clusters --query 'clusterArns[?contains(@,`native`)]'
aws s3 ls | grep native || echo "no native buckets"
```

Expected: empty lists and `no native buckets`.

```bash
make up-aws
```

Expected: preflight passes, apply completes, image pushes, service stabilises, and the script
prints `up.`

- [ ] **Step 3: Re-run the acceptance queries against the rebuilt stack**

Wait five minutes, then run all three queries from `awsnative/sql/verify_bronze.sql` again
(Task 12).

Expected: the same shape of results — both venues, no nulls, sane values. That the data is
*new* is the point: nothing was restored, it was re-derived from the live stream.

- [ ] **Step 4: Final check**

```bash
make check
```

Expected: green.

---

## Stage N1 — Definition of Done

Tick these only against observed output, not against expectation:

- [ ] `./scripts/native_preflight.sh` exits 0.
- [ ] `make check` is green (lint, mypy including `awsnative`, all unit tests).
- [ ] `make validate-aws` reports the configuration valid and `terraform fmt` clean.
- [ ] The Task 5 tripwire has been *seen* to fail on an added field, and then pass again.
- [ ] `make down-aws && make up-aws` completes from empty with no manual step.
- [ ] Both `binance` and `coinbase` appear in acceptance query 1 with non-zero rows.
- [ ] Acceptance query 2 returns `0` for every `null_*` column.
- [ ] Acceptance query 3 returns `0` for every column.
- [ ] Kinesis `IncomingRecords` is non-zero and rising in CloudWatch.
- [ ] The budget alarm exists and has your email on it.

## What comes next

Stages N2–N6 each get their own plan, in this order and for these reasons:

| Stage | Ships | Why next |
|---|---|---|
| N2 | Silver merge, expectations, quarantine | Bronze is unusable for analysis until dedupe exists; and the source-dependent recency bound (spec §5.4) needs settling before backfill lands late rows |
| N3 | `gold_bars_1m`, dirty-partition rebuild | First queryable product; establishes the two-step Step Functions pattern that replaces CDF |
| N4 | Backfill tiers, manifest, reconciliation | The durability model. Until this exists the weekly wipe is real data loss, not a rebuild |
| N5 | Prepared statements, tool server, IAM boundary | The anti-lookahead property, testable independently of any agent |
| N6 | Agent, decision persistence, dashboard | Closes the loop; needs N4's history and N5's boundary |

Two decisions to settle **before** starting N2, because they are cheap to answer now and
expensive to discover later: confirm Athena `MERGE INTO` on Iceberg works in your region
(spec §11 A2 — a one-query test), and confirm Claude Platform on AWS is available in-region
and its Marketplace subscription is permitted (spec §11 A4 — needed by N6 but with the
longest lead time if the answer is no).

---

## Suggested commit messages (optional, for when you're ready)

Not instructions to run — just good messages already written, in case you want them, grouped
to match the tasks above. Split them into as many or as few commits as you like.

**Preflight script:**
```
feat(native): add an AWS service preflight probe

Answers spec section 11 A5 empirically rather than by assumption. The IAM
probe creates and deletes a role because listing roles proves nothing about
whether creating one is permitted, and every service in this stack needs a
service role.
```

**Terraform root + state backend:**
```
feat(native): add the AWS-native Terraform root

Shares the MSK stack's state bucket and lock table under key
native/terraform.tfstate, so either stack can be destroyed without touching
the other. Ships a monthly budget alarm first because a weekly-wiped account
still bills for the hours it ran.
```

**Network module:**
```
feat(native): add the network module

Public subnets with assign_public_ip rather than private subnets behind a NAT
gateway: the producer needs outbound WSS and nothing inbound, and a NAT would
cost more per month than the rest of this stack combined.

A new module rather than reusing infra/modules/network, which requires a
kafka_client_cidrs input this stack has no opinion about and creates an MSK
security group nothing here would attach to. 10.43/16 avoids the MSK VPC's
10.42/16 so peering stays possible.
```

**Sink protocol extraction:**
```
refactor(ingest): extract the Sink protocol from runner

IngestRunner already depended on a structural protocol rather than on Kafka,
so the transport seam the AWS-native workstream needs already existed -- this
only moves the definition somewhere both implementations can import it.
ProducerLike had no call sites outside runner.py, so no behaviour changes.
```

**JSON encoder + tripwire:**
```
feat(native): add the JSON wire format and its drift tripwire

Kinesis records have no headers, so schema_version moves into the record body
and the envelope changes regardless -- given that, JSON lets Firehose convert
to Parquet natively and removes a transform Lambda from the hot path.

trade.v1.avsc stays the single source of truth: a contract test validates this
encoder's output against it and asserts the field sets are equal, so a rename
or an added field fails CI rather than Firehose delivery. Verified by watching
the tripwire fail on a deliberately added field.
```

**KinesisSink:**
```
feat(native): add KinesisSink

Two behaviours are load-bearing rather than incidental. put_records returns
HTTP 200 with a partial failure and the failed records are gone unless resent,
so only the failed subset is retried -- resending the whole batch would
duplicate the successes. And when retries are exhausted it raises rather than
dropping: the exception stops the drain, fills the bounded queue, and the
BLOCK policy for trades turns saturation into a detectable, REST-repairable
gap. Silent trade loss is the one failure this system must not have.

poll() is the batching trigger because IngestRunner.drain already calls it
after every produce.
```

**Producer entrypoint:**
```
feat(native): add the AWS-native producer entrypoint

A separate module rather than a transport flag on ingest.cli: the Kafka path
must keep starting with no AWS configuration present, and the image selects a
transport by choosing a command. Its own FDAI_NATIVE_ env prefix so neither
path can be started with the other's config.

One sink per venue so a throttled retry on one venue's batch never stalls the
other's drain task. Everything below the sink is shared, which is what makes
the two workstreams comparable rather than merely similar.
```

**Lakehouse module:**
```
feat(native): add the lakehouse module

S3 bucket, Glue database, the Bronze table, and an Athena workgroup. Bronze is
plain Parquet rather than Iceberg because it is append-only: no MERGE, no time
travel, no snapshot expiry to buy (spec D1).

Partition projection instead of a Glue crawler. Athena computes partition
locations from a declared pattern, which removes a component and with it any
window where S3 holds a partition the catalog has not registered.

The table is created before Firehose because Firehose reads this schema to know
how to write Parquet. Verified by querying the empty table: SUCCEEDED with 0
rows proves the SerDe and projection are right independently of any data.
```

**Stream module:**
```
feat(native): add the stream module -- Kinesis and Firehose

Kinesis on-demand rather than provisioned shards: steady state is ~300 msg/s
but crypto bursts 5-10x, and a single shard's 1000 rec/s cap would throttle
exactly when the data matters most. Two provisioned shards stays the documented
cost lever, not the default.

Firehose converts JSON to Parquet against the Glue table's schema, so no
transform Lambda sits in the hot path. The ingest_date directories come from a
time-based custom prefix (free) rather than dynamic partitioning (billed per
GB), and partition on arrival date so a late record never rewrites an old
partition.

Verified end to end with a single hand-placed record read back through Athena,
before any container exists -- so a later failure is attributable to the
producer rather than to the pipe.
```

**Producer image + Fargate service:**
```
feat(native): add the producer image and run it on ECS Fargate

A separate Dockerfile rather than editing docker/Dockerfile: the awsnative
dependency group is opt-in, and the Kafka image must not carry ~50MB of boto3
it never imports. No credentials in the image -- the ECS task role supplies
them at runtime through the container credential endpoint.

Two IAM roles because there are two actors: the execution role is used by the
ECS agent before the container starts (ECR pull, log stream), the task role by
the running code (kinesis:PutRecords on one stream, nothing else).

desired_count = 1 deliberately -- two tasks would each open their own exchange
connections and publish every trade twice. Deployment is stop-then-start rather
than rolling for the same reason: a brief gap is detectable and repairable,
where a brief duplicate is a silent volume error until dedupe runs.

Build must pass --platform linux/amd64: an arm64 image from Apple Silicon
pushes cleanly and then dies in Fargate with 'exec format error', a message
that names nothing about architecture.
```

**Acceptance queries:**
```
test(native): add the stage N1 acceptance queries

Three queries checking three different failure modes: rows from both venues
(a missing venue means a connector failed silently), no systematic nulls
(Firehose's OpenX deserializer writes NULL for a JSON key that does not match
a Glue column, so a rename shows up here and nowhere else), and sane values
(a working pipe carrying wrong numbers).

The null check is the one the encoder contract test cannot cover, because the
Glue table is a third party to the encoder/avsc agreement.
```

**`make up-aws`/`down-aws`:**
```
feat(native): add make up-aws / down-aws

Collapses the hand-run stage N0-N1 steps into one ordered command. The order
is load-bearing: Terraform will create the ECS service before any image exists
in ECR and the service then sits retrying CannotPullContainerError, so it is
apply, then push, then force-new-deployment.

Verified by destroying the stack and rebuilding from empty, then re-running the
acceptance queries -- which is also the test that nothing was created by hand
and left out of Terraform.
```
