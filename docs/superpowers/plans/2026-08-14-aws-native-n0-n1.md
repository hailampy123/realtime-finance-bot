# AWS-Native Workstream — Stages N0 + N1 Implementation Plan

> **For you, executing this by hand.** Steps use checkbox (`- [ ]`) syntax — tick them as
> you go. Each task ends with a verification step that either passes or fails; do not move
> on from a red one. Every task has a **Concepts** block explaining the AWS services it
> introduces in plain language, because the point of building this by hand is understanding
> the platform, not producing the artifact.

**Goal:** Live Binance and Coinbase trades flowing into S3 as Parquet, queryable in Athena,
with the whole stack reproducible from an empty AWS account by `make up-aws`.

**Architecture:** Reused `ingest/` connectors run as one ECS Fargate task writing JSON to
Kinesis Data Streams. Firehose converts JSON→Parquet and lands it in S3 partitioned by
arrival date. A Glue table with partition projection makes it queryable in Athena with no
crawler.

**Tech Stack:** Terraform ~> 5.0 AWS provider, Python 3.12 + boto3, ECS Fargate, ECR,
Kinesis Data Streams, Kinesis Data Firehose, S3, Glue Data Catalog, Athena.

**Spec:** [`2026-08-14-aws-native-workstream-design.md`](../specs/2026-08-14-aws-native-workstream-design.md)
— read §3, §4 and §5.1 before starting. This plan implements stages N0 and N1 of §12.

## Global Constraints

- Terraform `required_version >= 1.9.0`, AWS provider `~> 5.0` — match the existing roots.
- Project prefix is `fdai`; every AWS-native resource is named `fdai-native-*` so the two
  workstreams never collide on a name.
- `default_tags` on the provider must be `{ Project, ManagedBy = "terraform", Ephemeral = "true" }`
  — the wipe-safety convention from `infra/envs/dev/main.tf`.
- Terraform state goes in the **existing** `fdai-tfstate-<account-id>` bucket under key
  `native/terraform.tfstate`, locked by the existing `fdai-tflock` DynamoDB table. Never
  reuse the `dev/terraform.tfstate` key — that is the MSK stack's state.
- `ingest/` must not gain an AWS dependency. All boto3 code lives under `awsnative/`.
- `ingest/schemas/trade.v1.avsc` stays the single source of truth for the record shape.
- Python: ruff line-length 100, target `py312`, lint rules `E,F,I,UP,B,ASYNC,RUF`.
- `make check` (lint + mypy + unit tests) must be green before every commit.
- Nothing durable lives in this account. Every resource must be destroyable by
  `make down-aws` and recreatable by `make up-aws`.

## File Structure

**New Python — the AWS-specific tree:**

| File | Responsibility |
|---|---|
| `ingest/core/sinks.py` | The `Sink` protocol, extracted from `runner.ProducerLike`. Transport-agnostic. |
| `awsnative/__init__.py` | Package marker. |
| `awsnative/encode.py` | `Trade` → JSON bytes, carrying `schema_version`. The only place the wire format is decided. |
| `awsnative/sink.py` | `KinesisSink` — batching, partition keying, partial-failure retry. |
| `awsnative/settings.py` | `NativeSettings` — env config for the AWS producer (`FDAI_NATIVE_` prefix). |
| `awsnative/cli.py` | Entrypoint: connectors + `KinesisSink` + `IngestRunner`. Mirrors `ingest/cli.py`. |

**New tests:**

| File | Responsibility |
|---|---|
| `tests/awsnative/test_encode.py` | JSON↔`.avsc` contract (the drift tripwire). |
| `tests/awsnative/test_sink.py` | Batching, flush triggers, partial-failure retry, backoff. |

**New Terraform — one module per concern:**

| Path | Responsibility |
|---|---|
| `infra/modules/native_network/` | VPC, IGW, 2 public subnets, one egress-only SG. |
| `infra/modules/native_stream/` | Kinesis stream, Firehose delivery stream, Firehose IAM role. |
| `infra/modules/native_lakehouse/` | S3 lake bucket, Glue database, Bronze Glue table, Athena workgroup. |
| `infra/modules/native_producer/` | ECR repo, ECS cluster, task definition, task + execution roles, service. |
| `infra/envs/native/` | Root module wiring the four together. Own backend key. |

**New scripts and Docker:**

| Path | Responsibility |
|---|---|
| `scripts/native_preflight.sh` | Probes every AWS service this stack needs. Answers spec §11 A5. |
| `scripts/native_up.sh` | Ordered bring-up: preflight → apply → image push → force new deployment. |
| `docker/Dockerfile.awsnative` | Producer image with the `awsnative` dependency group. |

**Why `native_network` is a new module rather than reusing `infra/modules/network`:** the
existing module requires a `kafka_client_cidrs` variable and creates an MSK security group
with Kafka ports. Reusing it would mean passing a Kafka-shaped input this stack has no
opinion about, and carrying a security group nothing attaches to. A 50-line module with one
job is cleaner than a shared module with two personalities.

---

## Task 1: Preflight — prove the account can build this stack

**Concepts.** Before writing infrastructure, find out what the account permits. A **Service
Control Policy (SCP)** is an organisation-level rule that can deny AWS actions no matter what
your IAM permissions say. The MSK workstream was built around the belief that
`iam:CreateRole` was denied here; that turned out to be wrong, and this whole stack depends
on it being allowed. Verify rather than assume: a failed probe now costs a minute, discovering
it in stage N1 Task 11 costs an afternoon.

**Files:**
- Create: `scripts/native_preflight.sh`

- [ ] **Step 1: Confirm which account and region you're in**

```bash
aws sts get-caller-identity
aws configure get region
```

Expected: an `Account` number and a region string. Write both down — the account number
appears in the state bucket name, and everything must live in **one** region. If the region is
empty, set it: `export AWS_REGION=ap-southeast-2` (or whichever you use), and add it to your
shell profile.

- [ ] **Step 2: Write the preflight script**

Create `scripts/native_preflight.sh`:

```bash
#!/usr/bin/env bash
# Probes every AWS service the AWS-native stack needs, before any of it is built.
# Read-only except the IAM probe, which creates and immediately deletes a role --
# the only way to actually test iam:CreateRole is to call it.
set -uo pipefail

REGION="${AWS_REGION:-$(aws configure get region)}"
FAIL=0

probe() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    printf '  ok    %s\n' "$label"
  else
    printf '  FAIL  %s\n' "$label"
    FAIL=1
  fi
}

printf 'Region: %s\nAccount: %s\n\nRead/list probes:\n' \
  "$REGION" "$(aws sts get-caller-identity --query Account --output text)"

probe "kinesis"        aws kinesis list-streams
probe "firehose"       aws firehose list-delivery-streams
probe "s3"             aws s3api list-buckets
probe "glue"           aws glue get-databases
probe "athena"         aws athena list-work-groups
probe "ecr"            aws ecr describe-repositories
probe "ecs"            aws ecs list-clusters
probe "ec2 (vpc)"      aws ec2 describe-vpcs
probe "logs"           aws logs describe-log-groups
probe "stepfunctions"  aws stepfunctions list-state-machines
probe "lambda"         aws lambda list-functions
probe "budgets"        aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)"

# The load-bearing probe. Everything in this stack needs a service role.
printf '\nIAM create probe (creates then deletes fdai-native-preflight):\n'
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"firehose.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if aws iam create-role --role-name fdai-native-preflight \
     --assume-role-policy-document "$TRUST" >/dev/null 2>&1; then
  printf '  ok    iam:CreateRole\n'
  aws iam delete-role --role-name fdai-native-preflight >/dev/null 2>&1 \
    && printf '  ok    iam:DeleteRole\n' \
    || { printf '  FAIL  iam:DeleteRole (role fdai-native-preflight left behind)\n'; FAIL=1; }
else
  printf '  FAIL  iam:CreateRole -- this stack cannot be built without it\n'
  FAIL=1
fi

printf '\n'
if [ "$FAIL" -eq 0 ]; then
  printf 'All probes passed.\n'
else
  printf 'One or more probes FAILED. See spec section 11 (A4, A5) for fallbacks.\n'
fi
exit "$FAIL"
```

- [ ] **Step 3: Make it executable and run it**

```bash
chmod +x scripts/native_preflight.sh
./scripts/native_preflight.sh
```

Expected: every line `ok`, exit 0.

If `iam:CreateRole` fails, **stop** — this stack is not buildable and you should re-read spec
§11 A5. If a single service fails (say `stepfunctions`), that service's stage is blocked but
N0/N1 may still proceed: N1 needs only kinesis, firehose, s3, glue, athena, ecr, ecs, ec2,
logs, and iam.

- [ ] **Step 4: Commit**

```bash
git add scripts/native_preflight.sh
git commit -m "feat(native): add an AWS service preflight probe

Answers spec section 11 A5 empirically rather than by assumption. The IAM
probe creates and deletes a role because listing roles proves nothing about
whether creating one is permitted, and every service in this stack needs a
service role."
```

---

## Task 2: Terraform root and the shared state backend

**Concepts.** **Terraform** describes infrastructure as files, then makes reality match them.
Its **state file** records what it has created, so it knows what to change or destroy next
time. State lives in S3 (shared, durable) with a **DynamoDB lock table** so two runs can't
corrupt it. You already have both from the MSK workstream — this stack reuses them under a
different **key** (a path inside the bucket), which is what keeps the two stacks
independently destroyable. A **root module** is the directory you run `terraform apply` in; it
wires together **child modules**, which are reusable folders of resources.

**Files:**
- Create: `infra/envs/native/versions.tf`, `variables.tf`, `terraform.tfvars.example`,
  `backend.tf.tftpl`, `main.tf`, `outputs.tf`, `.gitignore`

**Interfaces:**
- Produces: root module variables `project`, `region`, `budget_notification_email`,
  `monthly_budget_usd`. Later tasks add `module` blocks to this root's `main.tf`.

- [ ] **Step 1: Create the directory and version pins**

```bash
mkdir -p infra/envs/native
```

Create `infra/envs/native/versions.tf`:

```hcl
terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Ephemeral = "true"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
```

- [ ] **Step 2: Create the variables**

Create `infra/envs/native/variables.tf`:

```hcl
variable "project" {
  description = "Resource name prefix. Shared with the MSK stack; native resources add a -native- infix."
  type        = string
  default     = "fdai"
}

variable "region" {
  type = string
}

variable "budget_notification_email" {
  description = "Where the monthly cost alarm sends mail. A wiped account still bills for the days it ran."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Alarm threshold. Spec section 10 estimates ~$25-35/mo at 30h/week."
  type        = number
  default     = 50
}
```

- [ ] **Step 3: Create the backend template and the tfvars example**

Create `infra/envs/native/backend.tf.tftpl`:

```hcl
terraform {
  backend "s3" {
    bucket         = "${state_bucket}"
    key            = "native/terraform.tfstate"
    region         = "${region}"
    dynamodb_table = "${lock_table}"
    encrypt        = true
  }
}
```

Create `infra/envs/native/terraform.tfvars.example`:

```hcl
region                    = "ap-southeast-2"
budget_notification_email = "you@example.com"
```

Create `infra/envs/native/.gitignore`:

```gitignore
backend.tf
terraform.tfvars
.terraform/
*.tfstate
*.tfstate.backup
```

- [ ] **Step 4: Add the budget alarm and an empty outputs file**

Create `infra/envs/native/main.tf`:

```hcl
# A wiped account still bills for the hours it ran, and per-GB streaming charges
# are the one line item here that scales with traffic rather than with time.
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-native-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}
```

Create `infra/envs/native/outputs.tf`:

```hcl
output "region" {
  value = data.aws_region.current.name
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}
```

- [ ] **Step 5: Render the backend and initialise**

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

- [ ] **Step 6: Verify the plan is clean and apply**

```bash
terraform -chdir=infra/envs/native plan
```

Expected: `Plan: 1 to add, 0 to change, 0 to destroy.` (just the budget).

```bash
terraform -chdir=infra/envs/native apply
```

Expected: `Apply complete! Resources: 1 added.`

- [ ] **Step 7: Look at what you made**

```bash
aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --query 'Budgets[?BudgetName==`fdai-native-monthly`]'
aws s3 ls "s3://fdai-tfstate-$(aws sts get-caller-identity --query Account --output text)/native/"
```

Expected: the budget JSON, and `terraform.tfstate` under the `native/` prefix — proving the
two stacks' states are separate objects in one bucket.

- [ ] **Step 8: Commit**

```bash
git add infra/envs/native/
git commit -m "feat(native): add the AWS-native Terraform root

Shares the MSK stack's state bucket and lock table under key
native/terraform.tfstate, so either stack can be destroyed without touching
the other. Ships a monthly budget alarm first because a weekly-wiped account
still bills for the hours it ran."
```

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

**Files:**
- Create: `infra/modules/native_network/{main.tf,variables.tf,outputs.tf}`
- Modify: `infra/envs/native/main.tf` (add the module block)

**Interfaces:**
- Produces: `module.network.public_subnet_ids` (list of string),
  `module.network.egress_security_group_id` (string), `module.network.vpc_id` (string).

- [ ] **Step 1: Write the module**

```bash
mkdir -p infra/modules/native_network
```

Create `infra/modules/native_network/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "vpc_cidr" {
  description = "Deliberately not 10.42.0.0/16 -- that is the MSK stack's VPC, and non-overlapping ranges keep peering an option later."
  type        = string
  default     = "10.43.0.0/16"
}

variable "az_count" {
  description = "Two so ECS has an alternative placement if one AZ is unavailable."
  type        = number
  default     = 2
}
```

Create `infra/modules/native_network/main.tf`:

```hcl
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.project}-native-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.project}-native-igw" }
}

resource "aws_subnet" "public" {
  count                   = var.az_count
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project}-native-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.project}-native-public" }
}

resource "aws_route_table_association" "public" {
  count          = var.az_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Egress only. The producer dials out to exchange WebSockets and to the Kinesis
# API; nothing ever connects to it. There is no inbound rule to review because
# there is no inbound rule.
resource "aws_security_group" "egress" {
  name        = "${var.project}-native-egress"
  description = "Fargate producer: egress only"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-native-egress" }
}
```

Create `infra/modules/native_network/outputs.tf`:

```hcl
output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "egress_security_group_id" {
  value = aws_security_group.egress.id
}
```

- [ ] **Step 2: Wire it into the root**

Append to `infra/envs/native/main.tf`:

```hcl
module "network" {
  source  = "../../modules/native_network"
  project = var.project
}
```

Append to `infra/envs/native/outputs.tf`:

```hcl
output "vpc_id" {
  value = module.network.vpc_id
}
```

- [ ] **Step 3: Apply**

```bash
terraform -chdir=infra/envs/native init   # picks up the new module
terraform -chdir=infra/envs/native apply
```

Expected: ~8 resources added (VPC, IGW, 2 subnets, route table, 2 associations, SG).

- [ ] **Step 4: Look at what you made — in the console this time**

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

- [ ] **Step 5: Commit**

```bash
git add infra/modules/native_network/ infra/envs/native/
git commit -m "feat(native): add the network module

Public subnets with assign_public_ip rather than private subnets behind a NAT
gateway: the producer needs outbound WSS and nothing inbound, and a NAT would
cost more per month than the rest of this stack combined.

A new module rather than reusing infra/modules/network, which requires a
kafka_client_cidrs input this stack has no opinion about and creates an MSK
security group nothing here would attach to. 10.43/16 avoids the MSK VPC's
10.42/16 so peering stays possible."
```

---

## Task 4: Extract the `Sink` protocol

**Concepts.** A Python **Protocol** is a structural interface: any class with matching method
signatures satisfies it, with no inheritance required. `IngestRunner` already depends on one
(`ProducerLike` in `ingest/runner.py:18`) rather than on Kafka, which means the seam this
whole workstream needs already exists. This task only moves the definition somewhere both
implementations can import it. `grep` confirms `ProducerLike` has no call sites outside
`runner.py`, so this is a zero-risk rename.

**Files:**
- Create: `ingest/core/sinks.py`
- Modify: `ingest/runner.py:5,18-21,36`

**Interfaces:**
- Produces: `ingest.core.sinks.Sink` — a Protocol with `produce(topic: str, trade: Trade) -> None`,
  `poll(timeout: float = 0.0) -> int`, `flush(timeout: float = 10.0) -> int`.

- [ ] **Step 1: Run the tests so you know they were green before you touched anything**

```bash
make test
```

Expected: all pass. Note the count — you will compare against it in Step 5.

- [ ] **Step 2: Create the protocol module**

Create `ingest/core/sinks.py`:

```python
"""The transport seam.

IngestRunner has always depended on a structural protocol rather than on Kafka,
so a second transport needs a second implementation and no change to the runner.
The protocol's shape is Kafka-derived and that turns out to fit: `poll` is called
after every `produce` in IngestRunner.drain, which is exactly the hook a batching
sink needs to decide whether to flush.

Implementations:
  ingest.core.producer.TradeProducer  -- Kafka/MSK, bare Avro datums
  awsnative.sink.KinesisSink          -- Kinesis Data Streams, JSON
"""

from __future__ import annotations

from typing import Protocol

from ingest.core.models import Trade


class Sink(Protocol):
    def produce(self, topic: str, trade: Trade) -> None:
        """Hand a trade to the transport. May buffer; must not block on the network."""
        ...

    def poll(self, timeout: float = 0.0) -> int:
        """Service the transport. Called after every produce and on drain idle.

        Returns the number of events serviced.
        """
        ...

    def flush(self, timeout: float = 10.0) -> int:
        """Block until buffered records are delivered. Returns the number still pending."""
        ...
```

- [ ] **Step 3: Point the runner at it**

In `ingest/runner.py`, delete the `ProducerLike` class (lines 18–21) and the now-unused
`Protocol` import on line 5, then add the new import and update the annotation.

The import block becomes:

```python
from ingest.connectors.base import Connector
from ingest.core.gaps import SequenceTracker
from ingest.core.models import Trade
from ingest.core.queue import BoundedTopicQueue
from ingest.core.sinks import Sink
from ingest.core.ws import ResilientWebSocket
```

and the constructor parameter becomes:

```python
        producer: Sink,
```

- [ ] **Step 4: Verify nothing else referenced the old name**

```bash
grep -rn "ProducerLike" --include="*.py" .
```

Expected: no output.

- [ ] **Step 5: Run the full check**

```bash
make check
```

Expected: lint clean, mypy clean, the same test count passing as in Step 1. A pure move
should change no behaviour, so any test change here is a bug in the move.

- [ ] **Step 6: Commit**

```bash
git add ingest/core/sinks.py ingest/runner.py
git commit -m "refactor(ingest): extract the Sink protocol from runner

IngestRunner already depended on a structural protocol rather than on Kafka,
so the transport seam the AWS-native workstream needs already existed -- this
only moves the definition somewhere both implementations can import it.
ProducerLike had no call sites outside runner.py, so no behaviour changes."
```

---

## Task 5: JSON encoder and the schema-drift tripwire

**Concepts.** Kafka records carry **headers** (out-of-band key/value metadata); Kinesis
records do **not**. The Kafka path puts `schema_version`, `venue`, `is_backfill`, and `source`
in headers — three of those are already Avro fields, so only `schema_version` needs a new
home inside the record body.

Since the envelope has to change anyway, the AWS path sends **JSON** rather than Avro. That
removes a whole component: Firehose converts JSON→Parquet natively against a Glue table
schema, so no Lambda sits in the hot path decoding Avro. The cost is a drift risk — two
independent definitions of the record shape. This task's test is what removes it: the JSON
is validated against `trade.v1.avsc` itself, so a field added on one side and not the other
fails CI rather than a 3 a.m. Firehose delivery.

We write the test first (TDD), so we know it can actually fail.

**Files:**
- Create: `awsnative/__init__.py`, `awsnative/encode.py`
- Create: `tests/awsnative/__init__.py`, `tests/awsnative/test_encode.py`
- Modify: `pyproject.toml` (package discovery + `awsnative` dependency group)

**Interfaces:**
- Produces: `awsnative.encode.encode_trade(trade: Trade) -> bytes` (UTF-8 JSON, no trailing
  newline) and `awsnative.encode.trade_to_dict(trade: Trade) -> dict[str, Any]`.

- [ ] **Step 1: Create the package skeleton**

```bash
mkdir -p awsnative tests/awsnative
touch awsnative/__init__.py tests/awsnative/__init__.py
```

- [ ] **Step 2: Add the package and dependency group to pyproject.toml**

In `[tool.setuptools.packages.find]`, add `awsnative*` to `include`:

```toml
include = ["ingest*", "backfill*", "devlab*", "lakehouse*", "awsnative*"]
```

Then add a new dependency group after the `lakehouse` group in `[dependency-groups]`:

```toml
# Not a default group: boto3 is ~50MB and only the AWS-native workstream needs
# it. `make check` and the Kafka producer image never install it, which is what
# keeps `ingest/` free of an AWS dependency in practice as well as by convention.
# Opt in with `uv sync --group awsnative`.
awsnative = [
    "boto3>=1.35",
    "botocore>=1.35",
]
```

Install it:

```bash
uv sync --group awsnative
```

- [ ] **Step 3: Write the failing test**

Create `tests/awsnative/test_encode.py`:

```python
from __future__ import annotations

import json

import fastavro
import pytest

from awsnative.encode import encode_trade, trade_to_dict
from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade


def make_trade(**overrides: object) -> Trade:
    base = dict(
        venue="binance",
        venue_symbol="BTCUSDT",
        instrument_id="BTC-USD",
        trade_id="12345",
        event_ts_us=1_754_000_000_000_000,
        ingest_ts_us=1_754_000_000_100_000,
        price="61234.56",
        size="0.0123",
        side=Side.BUY,
        sequence=987,
        is_backfill=False,
        source=Source.STREAM,
    )
    base.update(overrides)
    return Trade(**base)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "trade",
    [
        make_trade(),
        make_trade(side=Side.SELL, sequence=None),
        make_trade(source=Source.ARCHIVE, is_backfill=True),
        make_trade(side=Side.UNKNOWN, source=Source.REST_REPAIR),
    ],
    ids=["plain", "no-sequence", "archive", "unknown-side"],
)
def test_json_payload_validates_against_the_avro_schema(trade: Trade) -> None:
    """The drift tripwire.

    trade.v1.avsc stays the single source of truth for the record shape. If a
    field is added to the schema and not to the encoder (or vice versa), this
    fails in CI rather than as a Firehose delivery error at 3am.
    """
    payload = trade_to_dict(trade)
    schema_fields = dict(payload)
    schema_fields.pop("schema_version")  # transport metadata, not part of the record

    assert fastavro.validate(schema_fields, trade_codec().schema, raise_errors=True)


def test_encoder_and_avro_schema_agree_on_field_names() -> None:
    """Catches a renamed field, which validate() alone would let through if the
    old name were simply absent and nullable."""
    payload = trade_to_dict(make_trade())
    encoder_fields = set(payload) - {"schema_version"}
    avro_fields = {f["name"] for f in trade_codec().schema["fields"]}

    assert encoder_fields == avro_fields


def test_schema_version_is_carried_in_the_body() -> None:
    """Kinesis has no headers, so the version the Kafka path puts in a header
    has to travel inside the record."""
    assert trade_to_dict(make_trade())["schema_version"] == TRADE_SCHEMA_VERSION


def test_encode_trade_is_compact_utf8_json_without_a_trailing_newline() -> None:
    """Firehose's OpenX JSON deserializer reads one JSON document per record.
    A trailing newline or pretty-printing is wasted bytes on a per-GB bill."""
    raw = encode_trade(make_trade())

    assert isinstance(raw, bytes)
    assert not raw.endswith(b"\n")
    assert b", " not in raw and b": " not in raw
    assert json.loads(raw.decode())["venue"] == "binance"


def test_enums_encode_as_their_string_values() -> None:
    payload = trade_to_dict(make_trade(side=Side.SELL, source=Source.ARCHIVE))

    assert payload["side"] == "SELL"
    assert payload["source"] == "ARCHIVE"
```

- [ ] **Step 4: Run it and watch it fail for the right reason**

```bash
uv run --group awsnative pytest tests/awsnative/test_encode.py -v
```

Expected: `ModuleNotFoundError: No module named 'awsnative.encode'`. If you get a different
error, fix that before writing the implementation — a test that fails for the wrong reason
proves nothing.

- [ ] **Step 5: Write the implementation**

Create `awsnative/encode.py`:

```python
"""Trade -> JSON, the AWS-native wire format.

The Kafka path sends bare Avro datums plus four Kafka headers. Kinesis records
have no headers, so the envelope has to change regardless; given that, JSON is
the better choice here because Firehose converts JSON to Parquet natively
against a Glue table schema. That removes a transform Lambda from the hot path.

trade.v1.avsc remains the single source of truth. Nothing in this module reads
the schema at runtime -- the guarantee is a contract test
(tests/awsnative/test_encode.py) asserting this output validates against it,
so drift fails CI instead of failing delivery.
"""

from __future__ import annotations

import json
from typing import Any

from ingest.core.codec import TRADE_SCHEMA_VERSION
from ingest.core.models import Trade

# Compact: no whitespace after separators. At ~350 bytes a record and a per-GB
# Kinesis and Firehose bill, pretty-printing is a line item.
_SEPARATORS = (",", ":")


def trade_to_dict(trade: Trade) -> dict[str, Any]:
    """The record as a plain dict, with schema_version added.

    Trade.to_avro() already stringifies the two StrEnums, so this is that dict
    plus the one field Kinesis's lack of headers forces into the body.
    """
    record = trade.to_avro()
    record["schema_version"] = TRADE_SCHEMA_VERSION
    return record


def encode_trade(trade: Trade) -> bytes:
    """UTF-8 JSON, one document, no trailing newline.

    Firehose's OpenX JSON deserializer reads exactly one JSON document per
    Kinesis record, so this must not emit newline-delimited batches.
    """
    return json.dumps(trade_to_dict(trade), separators=_SEPARATORS).encode()
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
uv run --group awsnative pytest tests/awsnative/test_encode.py -v
```

Expected: 8 passed (5 test functions, one parametrised 4 ways).

- [ ] **Step 7: Prove the tripwire actually trips**

This is the important step. A guard you haven't seen fail is an assumption.

```bash
# Temporarily add a field to the encoder that the schema does not have
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
git checkout awsnative/encode.py 2>/dev/null || python - <<'EOF'
import pathlib
p = pathlib.Path("awsnative/encode.py")
p.write_text(p.read_text().replace('\n    record["bogus_field"] = 1', ''))
EOF
uv run --group awsnative pytest tests/awsnative/test_encode.py -q
```

Expected: back to 8 passed.

- [ ] **Step 8: Make sure the default test run still works without boto3**

The `awsnative` group is opt-in, so plain `make test` must not import boto3. `encode.py`
doesn't, so these tests should pass either way:

```bash
make check
```

Expected: green, with the encode tests included.

- [ ] **Step 9: Commit**

```bash
git add awsnative/ tests/awsnative/ pyproject.toml uv.lock
git commit -m "feat(native): add the JSON wire format and its drift tripwire

Kinesis records have no headers, so schema_version moves into the record body
and the envelope changes regardless -- given that, JSON lets Firehose convert
to Parquet natively and removes a transform Lambda from the hot path.

trade.v1.avsc stays the single source of truth: a contract test validates this
encoder's output against it and asserts the field sets are equal, so a rename
or an added field fails CI rather than Firehose delivery. Verified by watching
the tripwire fail on a deliberately added field."
```

---

## Task 6: The Kinesis sink

**Concepts.** **Kinesis Data Streams** is a durable, ordered log — Kafka's closest AWS
analogue. Records are grouped into **shards**; a record's **partition key** decides its shard,
so all records with the same key stay ordered relative to each other. We reuse
`Trade.kafka_key()` (`venue|symbol`) so one instrument on one venue keeps its ordering, the
same property the Kafka key bought. This stream is **on-demand**, so AWS manages shard count.

`put_records` sends up to 500 records per call. The gotcha that catches everyone: **it can
partially fail.** You get HTTP 200 with a `FailedRecordCount` and per-record error codes, and
the failed records are simply gone unless you resend them. Silently dropping them here would
break the one property the spec says this system must not have — silent trade loss. So the
sink retries only the failed subset, with exponential backoff, and blocks rather than drops
when it can't keep up. Blocking propagates as queue backpressure, which
`BoundedTopicQueue`'s BLOCK policy turns into a *detectable, REST-repairable gap*.

**Files:**
- Create: `awsnative/sink.py`
- Create: `tests/awsnative/test_sink.py`

**Interfaces:**
- Consumes: `awsnative.encode.encode_trade`, `ingest.core.sinks.Sink`,
  `ingest.core.models.Trade`.
- Produces: `awsnative.sink.KinesisSink(stream_name: str, *, client: Any | None = None,
  max_batch: int = 500, max_bytes: int = 4_500_000, max_attempts: int = 8,
  sleep: Callable[[float], None] = time.sleep)` implementing `Sink`.

- [ ] **Step 1: Write the failing test**

Create `tests/awsnative/test_sink.py`:

```python
from __future__ import annotations

import json
from typing import Any

import pytest

from awsnative.sink import KinesisSink
from ingest.core.models import Side, Source, Trade


def make_trade(trade_id: str = "1", symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        venue="binance",
        venue_symbol=symbol,
        instrument_id="BTC-USD",
        trade_id=trade_id,
        event_ts_us=1_754_000_000_000_000,
        ingest_ts_us=1_754_000_000_100_000,
        price="61234.56",
        size="0.0123",
        side=Side.BUY,
        sequence=int(trade_id),
        is_backfill=False,
        source=Source.STREAM,
    )


class FakeKinesis:
    """Records every put_records call and replays a scripted set of outcomes."""

    def __init__(self, failures: list[set[int]] | None = None) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        # failures[n] = indices within call n that should fail
        self._failures = failures or []

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> dict[str, Any]:
        self.stream_name = StreamName
        self.calls.append(Records)
        failing = self._failures[len(self.calls) - 1] if len(self.calls) <= len(self._failures) else set()
        return {
            "FailedRecordCount": len(failing),
            "Records": [
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "slow down"}
                if i in failing
                else {"ShardId": "shardId-000000000000", "SequenceNumber": str(i)}
                for i in range(len(Records))
            ],
        }


def make_sink(fake: FakeKinesis, **kwargs: Any) -> tuple[KinesisSink, list[float]]:
    slept: list[float] = []
    sink = KinesisSink(
        "fdai-native-md-trades-v1",
        client=fake,
        sleep=slept.append,
        **kwargs,
    )
    return sink, slept


def test_produce_buffers_and_does_not_call_aws() -> None:
    """produce must never block on the network -- IngestRunner calls it from the
    frame-parsing path."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake)

    sink.produce("md.trades.v1", make_trade())

    assert fake.calls == []


def test_flush_sends_the_buffer_and_empties_it() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade("1"))
    sink.produce("md.trades.v1", make_trade("2"))

    pending = sink.flush()

    assert pending == 0
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 2
    assert fake.stream_name == "fdai-native-md-trades-v1"


def test_partition_key_is_venue_pipe_symbol() -> None:
    """Same key as the Kafka path, so one instrument on one venue stays ordered."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade(symbol="ETHUSDT"))
    sink.flush()

    assert fake.calls[0][0]["PartitionKey"] == "binance|ETHUSDT"


def test_record_data_is_the_json_encoding() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade("42"))
    sink.flush()

    payload = json.loads(fake.calls[0][0]["Data"].decode())
    assert payload["trade_id"] == "42"
    assert payload["schema_version"] == 1


def test_poll_flushes_once_the_batch_is_full() -> None:
    """poll is called after every produce in IngestRunner.drain, which makes it
    the natural place to decide whether to send."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=3)

    for i in range(3):
        sink.produce("md.trades.v1", make_trade(str(i)))
        sink.poll(0)

    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 3


def test_poll_does_not_flush_a_partial_batch() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=10)

    sink.produce("md.trades.v1", make_trade())
    sink.poll(0)

    assert fake.calls == []


def test_only_failed_records_are_retried() -> None:
    """The Kinesis gotcha: put_records returns 200 with a partial failure, and
    the failed records are lost unless resent. Resending the whole batch would
    duplicate the successes."""
    fake = FakeKinesis(failures=[{1}])  # second record of the first call fails
    sink, slept = make_sink(fake)
    for i in range(3):
        sink.produce("md.trades.v1", make_trade(str(i)))

    pending = sink.flush()

    assert pending == 0
    assert len(fake.calls) == 2
    assert len(fake.calls[1]) == 1
    retried = json.loads(fake.calls[1][0]["Data"].decode())
    assert retried["trade_id"] == "1"
    assert slept, "a throttled retry must back off before resending"


def test_backoff_grows_between_attempts() -> None:
    fake = FakeKinesis(failures=[{0}, {0}, {0}])
    sink, slept = make_sink(fake)
    sink.produce("md.trades.v1", make_trade())

    sink.flush()

    assert len(slept) >= 3
    assert slept[1] > slept[0]


def test_exhausting_retries_raises_rather_than_dropping() -> None:
    """Silent trade loss is the one failure this system must not have. Raising
    surfaces as queue backpressure, which BoundedTopicQueue's BLOCK policy turns
    into a detectable, REST-repairable gap."""
    fake = FakeKinesis(failures=[{0}] * 10)
    sink, _ = make_sink(fake, max_attempts=3)
    sink.produce("md.trades.v1", make_trade())

    with pytest.raises(RuntimeError, match="1 record"):
        sink.flush()


def test_oversized_buffer_flushes_on_bytes_not_just_count() -> None:
    """put_records caps at 5MB per request; exceeding it fails the whole call."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=500, max_bytes=700)

    for i in range(4):
        sink.produce("md.trades.v1", make_trade(str(i)))
        sink.poll(0)

    assert fake.calls, "should have flushed on the byte threshold"
    assert all(
        sum(len(r["Data"]) for r in call) <= 700 + 400 for call in fake.calls
    ), "each request must respect the byte cap"
```

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run --group awsnative pytest tests/awsnative/test_sink.py -q
```

Expected: `ModuleNotFoundError: No module named 'awsnative.sink'`.

- [ ] **Step 3: Write the implementation**

Create `awsnative/sink.py`:

```python
"""KinesisSink -- the AWS-native implementation of ingest.core.sinks.Sink.

Two behaviours here are load-bearing rather than incidental.

Partial failures. put_records returns HTTP 200 with a FailedRecordCount and
per-record error codes; the failed records are gone unless resent. Resending
the whole batch would duplicate the successes, so only the failed subset is
retried, matched positionally against the request.

Never dropping a trade. When retries are exhausted this raises instead of
discarding. The exception propagates to IngestRunner.drain and stops the drain,
which fills the BoundedTopicQueue, whose BLOCK policy for trades makes the
producer block and take a gap -- and a gap is detectable and REST-repairable,
where a silent drop is not. That chain is why the parent spec can claim trades
are never silently lost, and it is inherited unchanged from the Kafka path.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

import structlog

from awsnative.encode import encode_trade
from ingest.core.models import Trade

log = structlog.get_logger(__name__)

# put_records hard limits: 500 records and 5 MiB per request. The byte cap is
# set below 5 MiB so a batch assembled right up to the threshold plus one more
# record still fits.
_MAX_RECORDS_PER_REQUEST = 500
_MAX_BYTES_PER_REQUEST = 4_500_000

# Throttling is expected on an on-demand stream: it doubles capacity within
# ~15 minutes of a sustained increase, so an instantaneous spike above current
# capacity is throttled regardless of stream mode.
_BASE_BACKOFF_S = 0.05
_MAX_BACKOFF_S = 5.0


def _default_client() -> Any:
    import boto3

    return boto3.client("kinesis")


class KinesisSink:
    def __init__(
        self,
        stream_name: str,
        *,
        client: Any | None = None,
        max_batch: int = _MAX_RECORDS_PER_REQUEST,
        max_bytes: int = _MAX_BYTES_PER_REQUEST,
        max_attempts: int = 8,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._stream_name = stream_name
        self._client = client if client is not None else _default_client()
        self._max_batch = max_batch
        self._max_bytes = max_bytes
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._buffer: list[dict[str, Any]] = []
        self._buffered_bytes = 0

    # -- Sink protocol -------------------------------------------------------

    def produce(self, topic: str, trade: Trade) -> None:
        """Buffer only. IngestRunner calls this from the frame-parsing path, so
        it must not touch the network. `topic` is accepted for protocol
        compatibility and ignored: a Kinesis stream is the topic."""
        data = encode_trade(trade)
        self._buffer.append(
            {"Data": data, "PartitionKey": trade.kafka_key().decode()}
        )
        self._buffered_bytes += len(data)

    def poll(self, timeout: float = 0.0) -> int:
        """Send if a threshold is reached. IngestRunner.drain calls this after
        every produce, which makes it the batching trigger."""
        if len(self._buffer) >= self._max_batch or self._buffered_bytes >= self._max_bytes:
            return self._send_all()
        return 0

    def flush(self, timeout: float = 10.0) -> int:
        """Send everything buffered. Returns records still pending (always 0 --
        anything undeliverable raises)."""
        self._send_all()
        return len(self._buffer)

    # -- internals -----------------------------------------------------------

    def _send_all(self) -> int:
        sent = 0
        while self._buffer:
            batch, self._buffer = self._take_batch()
            self._buffered_bytes = sum(len(r["Data"]) for r in self._buffer)
            sent += self._send_with_retry(batch)
        return sent

    def _take_batch(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Slice off one request-sized batch, respecting both caps."""
        batch: list[dict[str, Any]] = []
        size = 0
        for index, record in enumerate(self._buffer):
            record_size = len(record["Data"])
            if batch and (len(batch) >= self._max_batch or size + record_size > self._max_bytes):
                return batch, self._buffer[index:]
            batch.append(record)
            size += record_size
        return batch, []

    def _send_with_retry(self, records: list[dict[str, Any]]) -> int:
        pending = records
        for attempt in range(self._max_attempts):
            if attempt:
                # Full jitter: a fixed backoff makes every retrying producer
                # collide on the same instant.
                delay = min(_MAX_BACKOFF_S, _BASE_BACKOFF_S * (2**attempt))
                self._sleep(random.uniform(0, delay))  # noqa: S311 -- jitter, not crypto

            response = self._client.put_records(
                StreamName=self._stream_name, Records=pending
            )
            failed_count = int(response.get("FailedRecordCount", 0) or 0)
            if not failed_count:
                return len(pending)

            results = response.get("Records", [])
            pending = [
                record
                for record, result in zip(pending, results, strict=False)
                if result.get("ErrorCode")
            ]
            log.warning(
                "kinesis_partial_failure",
                failed=len(pending),
                attempt=attempt + 1,
                error_code=next(
                    (r.get("ErrorCode") for r in results if r.get("ErrorCode")), None
                ),
            )
            if not pending:
                return len(records)

        raise RuntimeError(
            f"kinesis put_records failed for {len(pending)} record(s) after "
            f"{self._max_attempts} attempts; refusing to drop trades"
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run --group awsnative pytest tests/awsnative/test_sink.py -v
```

Expected: 10 passed.

- [ ] **Step 5: Verify the protocol is actually satisfied**

Structural typing means a signature mismatch is only caught where the two meet. mypy checks
this — but only if `awsnative` is in the typecheck target.

In `Makefile`, change the `typecheck` target to include the new package:

```make
typecheck:
	uv run --group lakehouse --group awsnative mypy ingest devlab lakehouse awsnative
```

Then add a compile-time assertion at the end of `awsnative/sink.py`:

```python
def _assert_protocol() -> None:
    """mypy fails here if KinesisSink drifts from the Sink protocol."""
    from ingest.core.sinks import Sink

    sink: Sink = KinesisSink("x", client=object())
    _ = sink
```

```bash
make typecheck
```

Expected: `Success: no issues found`.

- [ ] **Step 6: Run the whole check**

```bash
make check
```

Expected: green.

- [ ] **Step 7: Commit**

```bash
git add awsnative/sink.py tests/awsnative/test_sink.py Makefile
git commit -m "feat(native): add KinesisSink

Two behaviours are load-bearing rather than incidental. put_records returns
HTTP 200 with a partial failure and the failed records are gone unless resent,
so only the failed subset is retried -- resending the whole batch would
duplicate the successes. And when retries are exhausted it raises rather than
dropping: the exception stops the drain, fills the bounded queue, and the
BLOCK policy for trades turns saturation into a detectable, REST-repairable
gap. Silent trade loss is the one failure this system must not have.

poll() is the batching trigger because IngestRunner.drain already calls it
after every produce."
```

---

## Task 7: The producer entrypoint

**Concepts.** Nothing new in AWS terms — this is the composition root for the AWS path,
mirroring `ingest/cli.py`. It exists as a separate module rather than a flag on the existing
CLI so that the Kafka path keeps working with no AWS config present, and so the container
image can select a transport by choosing a command.

**Files:**
- Create: `awsnative/settings.py`, `awsnative/cli.py`

**Interfaces:**
- Consumes: `awsnative.sink.KinesisSink`, `ingest.runner.IngestRunner`,
  `ingest.core.instruments.InstrumentMap`, `ingest.core.queue.BoundedTopicQueue`.
- Produces: `python -m awsnative.cli` as the container command.

- [ ] **Step 1: Write the settings module**

Create `awsnative/settings.py`:

```python
from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute for the same reason ingest/settings.py is: a process whose cwd is
# not the repo root would otherwise silently read a different .env.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class NativeSettings(BaseSettings):
    """Config for the AWS-native producer.

    Separate from ingest.Settings, and a separate env prefix, so that neither
    path can be started with the other's configuration by accident -- there is
    no bootstrap_servers here to leave pointing at a dead broker.
    """

    model_config = SettingsConfigDict(env_prefix="FDAI_NATIVE_", env_file=_ENV_FILE)

    stream_name: str = "fdai-native-md-trades-v1"
    universe_path: Path = Path("config/universe.yaml")
    venues: list[str] = ["binance", "coinbase"]
    queue_maxsize: int = 20_000
    trades_topic: str = "md.trades.v1"
```

- [ ] **Step 2: Write the CLI**

Create `awsnative/cli.py`:

```python
"""AWS-native producer entrypoint.

Deliberately a separate module from ingest.cli rather than a transport flag on
it: the Kafka path must keep starting with no AWS configuration present, and
the container image selects a transport by choosing a command.

Everything below the sink is shared -- same connectors, same gap detection and
REST repair, same backpressure policy -- which is what makes the two
workstreams comparable rather than merely similar.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Callable

import structlog

from awsnative.settings import NativeSettings
from awsnative.sink import KinesisSink
from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue
from ingest.runner import IngestRunner

log = structlog.get_logger(__name__)

CONNECTORS: dict[str, Callable[[InstrumentMap], Connector]] = {
    "binance": BinanceConnector,
    "coinbase": CoinbaseConnector,
}


def build_connector(venue: str, instruments: InstrumentMap) -> Connector:
    try:
        return CONNECTORS[venue](instruments)
    except KeyError:
        raise SystemExit(f"unknown venue {venue!r}; known: {sorted(CONNECTORS)}") from None


async def main() -> None:
    structlog.configure(processors=[structlog.processors.JSONRenderer()])
    settings = NativeSettings()
    instruments = InstrumentMap.from_yaml(settings.universe_path)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    tasks = []
    for venue in settings.venues:
        connector = build_connector(venue, instruments)
        symbols = instruments.symbols_for(venue)
        if not symbols:
            log.warning("no_symbols_configured", venue=venue)
            continue
        runner = IngestRunner(
            connector,
            # One sink per venue: each has its own buffer, so a throttled retry
            # on one venue's batch never stalls the other's drain task.
            KinesisSink(settings.stream_name),
            SequenceTracker(),
            BoundedTopicQueue(settings.queue_maxsize, TOPIC_POLICIES[settings.trades_topic]),
            settings.trades_topic,
        )
        log.info(
            "starting_venue",
            venue=venue,
            symbols=len(symbols),
            stream=settings.stream_name,
        )
        tasks.append(asyncio.create_task(runner.run(stop, symbols)))

    if not tasks:
        raise SystemExit("no venues to run")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
```

- [ ] **Step 3: Verify it imports and fails cleanly with no AWS credentials**

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

- [ ] **Step 4: Run the check**

```bash
make check
```

Expected: green.

- [ ] **Step 5: Commit**

```bash
git add awsnative/settings.py awsnative/cli.py
git commit -m "feat(native): add the AWS-native producer entrypoint

A separate module rather than a transport flag on ingest.cli: the Kafka path
must keep starting with no AWS configuration present, and the image selects a
transport by choosing a command. Its own FDAI_NATIVE_ env prefix so neither
path can be started with the other's config.

One sink per venue so a throttled retry on one venue's batch never stalls the
other's drain task. Everything below the sink is shared, which is what makes
the two workstreams comparable rather than merely similar."
```

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

Create the Glue table **before** Firehose, because Firehose reads this table's schema to know
how to write Parquet.

**Files:**
- Create: `infra/modules/native_lakehouse/{main.tf,variables.tf,outputs.tf}`
- Modify: `infra/envs/native/main.tf`, `infra/envs/native/outputs.tf`

**Interfaces:**
- Produces: `module.lakehouse.bucket_arn`, `bucket_name`, `glue_database_name`,
  `bronze_table_name`, `athena_workgroup_name`, `athena_results_prefix`.

- [ ] **Step 1: Write the variables**

```bash
mkdir -p infra/modules/native_lakehouse
```

Create `infra/modules/native_lakehouse/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "account_id" {
  type = string
}

variable "projection_start_date" {
  description = <<-DESC
    Lower bound for Athena partition projection on ingest_date. Projection
    computes partition locations arithmetically instead of listing them, so this
    bounds the search space -- set it to roughly when the account was created,
    not to 1970, or every query enumerates decades of nonexistent partitions.
  DESC
  type        = string
  default     = "2026-01-01"
}
```

Create `infra/modules/native_lakehouse/main.tf`:

```hcl
locals {
  bucket_name  = "${var.project}-native-lake-${var.account_id}"
  bronze_table = "bronze_trades_stream"
  bronze_prefix = "bronze_trades_stream"
}

# force_destroy because this bucket holds only re-derivable data (spec section 6)
# and `make down-aws` must not fail on a non-empty bucket.
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Athena query results and Firehose error output accumulate and are never read
# after the fact; expiring them keeps the bill honest.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "_athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-firehose-errors"
    status = "Enabled"
    filter {
      prefix = "_errors/"
    }
    expiration {
      days = 14
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = "${replace(var.project, "-", "_")}_native"
  description = "AWS-native workstream lakehouse"
}

# Bronze is plain Parquet, not Iceberg: it is append-only, so MERGE, time travel
# and snapshot expiry all buy nothing (spec D1).
resource "aws_glue_catalog_table" "bronze" {
  name          = local.bronze_table
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    "parquet.compression" = "SNAPPY"

    # Partition projection: Athena computes partition locations from this pattern
    # instead of listing them, which removes the Glue crawler entirely and with
    # it any window where S3 has a partition the catalog does not.
    "projection.enabled"                   = "true"
    "projection.ingest_date.type"          = "date"
    "projection.ingest_date.format"        = "yyyy-MM-dd"
    "projection.ingest_date.range"         = "${var.projection_start_date},NOW"
    "projection.ingest_date.interval"      = "1"
    "projection.ingest_date.interval.unit" = "DAYS"
    # $${} escapes Terraform interpolation -- Athena needs the literal ${ingest_date}.
    "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/${local.bronze_prefix}/ingest_date=$${ingest_date}/"
  }

  partition_keys {
    name = "ingest_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/${local.bronze_prefix}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Must match awsnative/encode.py field-for-field. The contract test in
    # tests/awsnative/test_encode.py keeps the encoder honest against
    # trade.v1.avsc; this table is the third party to that agreement, and
    # Task 12's verification query is what proves it lines up.
    columns { name = "venue"          type = "string" }
    columns { name = "venue_symbol"   type = "string" }
    columns { name = "instrument_id"  type = "string" }
    columns { name = "trade_id"       type = "string" }
    columns { name = "event_ts_us"    type = "bigint" }
    columns { name = "ingest_ts_us"   type = "bigint" }
    columns { name = "price"          type = "string" }
    columns { name = "size"           type = "string" }
    columns { name = "side"           type = "string" }
    columns { name = "sequence"       type = "bigint" }
    columns { name = "is_backfill"    type = "boolean" }
    columns { name = "source"         type = "string" }
    columns { name = "schema_version" type = "int" }
  }
}

resource "aws_athena_workgroup" "native" {
  name          = "${var.project}-native"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/_athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
```

Create `infra/modules/native_lakehouse/outputs.tf`:

```hcl
output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.lake.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lake.name
}

output "bronze_table_name" {
  value = aws_glue_catalog_table.bronze.name
}

output "bronze_prefix" {
  value = local.bronze_prefix
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.native.name
}
```

- [ ] **Step 2: Wire it into the root**

Append to `infra/envs/native/main.tf`:

```hcl
module "lakehouse" {
  source     = "../../modules/native_lakehouse"
  project    = var.project
  account_id = data.aws_caller_identity.current.account_id
}
```

Append to `infra/envs/native/outputs.tf`:

```hcl
output "lake_bucket" {
  value = module.lakehouse.bucket_name
}

output "glue_database" {
  value = module.lakehouse.glue_database_name
}

output "athena_workgroup" {
  value = module.lakehouse.athena_workgroup_name
}
```

- [ ] **Step 3: Apply**

```bash
terraform -chdir=infra/envs/native init
terraform -chdir=infra/envs/native apply
```

Expected: ~7 resources added.

- [ ] **Step 4: Query the empty table — proving the catalog works before any data exists**

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

- [ ] **Step 5: Look at the table definition in the console**

Open the Athena console → **Query editor** → set Workgroup to `fdai-native` and Database to
`fdai_native`. Expand `bronze_trades_stream` in the left pane and confirm the 13 columns plus
the `ingest_date` partition key. Then Glue console → **Tables** → `bronze_trades_stream` →
**Table properties**, and read the `projection.*` keys — this is where you can see that no
partition is *registered*, only *described*.

- [ ] **Step 6: Commit**

```bash
git add infra/modules/native_lakehouse/ infra/envs/native/
git commit -m "feat(native): add the lakehouse module

S3 bucket, Glue database, the Bronze table, and an Athena workgroup. Bronze is
plain Parquet rather than Iceberg because it is append-only: no MERGE, no time
travel, no snapshot expiry to buy (spec D1).

Partition projection instead of a Glue crawler. Athena computes partition
locations from a declared pattern, which removes a component and with it any
window where S3 holds a partition the catalog has not registered.

The table is created before Firehose because Firehose reads this schema to know
how to write Parquet. Verified by querying the empty table: SUCCEEDED with 0
rows proves the SerDe and projection are right independently of any data."
```

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

**Files:**
- Create: `infra/modules/native_stream/{main.tf,variables.tf,outputs.tf}`
- Modify: `infra/envs/native/main.tf`, `infra/envs/native/outputs.tf`

**Interfaces:**
- Consumes: `module.lakehouse.{bucket_arn,glue_database_name,bronze_table_name,bronze_prefix}`.
- Produces: `module.stream.stream_name`, `module.stream.stream_arn`,
  `module.stream.firehose_log_group`.

- [ ] **Step 1: Write the variables**

```bash
mkdir -p infra/modules/native_stream
```

Create `infra/modules/native_stream/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "bronze_table_name" {
  type = string
}

variable "bronze_prefix" {
  type = string
}

variable "retention_hours" {
  description = "24h matches the Kafka topic's retention, so the two workstreams lose the same amount on teardown."
  type        = number
  default     = 24
}

variable "buffer_interval_seconds" {
  description = <<-DESC
    Firehose writes when size OR interval is hit. 120s puts producer->Bronze lag
    around 2 minutes, inside the parent spec's p50 < 6 min SLO once the 5-minute
    merge cadence is added. Raise it for fewer, larger files; lower it for
    fresher data.
  DESC
  type        = number
  default     = 120
}

variable "buffer_size_mb" {
  description = "Minimum is 64 when data format conversion is enabled."
  type        = number
  default     = 128
}
```

- [ ] **Step 2: Write the stream and the Firehose role**

Create `infra/modules/native_stream/main.tf`:

```hcl
locals {
  stream_name   = "${var.project}-native-md-trades-v1"
  firehose_name = "${var.project}-native-bronze-trades"
}

# On-demand rather than provisioned shards. Steady state is ~300 msg/s, but
# crypto trade rates burst 5-10x during volatility and a single provisioned
# shard caps at 1000 records/s -- it would throttle exactly when the data
# matters most. Two provisioned shards is the documented cost lever
# (~$3/mo cheaper, spec section 10), not the default: shard math is a thing to
# get wrong, and getting it wrong drops trades.
resource "aws_kinesis_stream" "trades" {
  name             = local.stream_name
  retention_period = var.retention_hours
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# --- Firehose service role -------------------------------------------------
# Two halves: a trust policy saying Firehose may assume this role, and
# permission policies saying what it may then do.

data "aws_iam_policy_document" "firehose_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    # Belt and braces: only this account's Firehose may assume it, so the role
    # cannot be used cross-account even if its ARN leaks.
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project}-native-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_trust.json
}

data "aws_iam_policy_document" "firehose_permissions" {
  # Read the stream it is sourced from.
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.trades.arn]
  }

  # Decrypt the stream, which is KMS-encrypted above.
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["arn:aws:kms:${var.region}:${var.account_id}:alias/aws/kinesis"]
  }

  # Write objects, and list the bucket (Firehose checks before writing).
  statement {
    effect  = "Allow"
    actions = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject",
               "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }

  # Read the Glue table schema -- this is how it knows how to write Parquet.
  statement {
    effect    = "Allow"
    actions   = ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/${var.bronze_table_name}",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/kinesisfirehose/*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.project}-native-firehose"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}
```

- [ ] **Step 3: Add the Firehose delivery stream**

Append to `infra/modules/native_stream/main.tf`:

```hcl
resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = local.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.trades.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.lake_bucket_arn

    # Time-based custom prefix, which is free. NOT dynamic partitioning, which
    # keys off record content and is billed per GB. Partitioning on arrival date
    # (not event date) is deliberate: it matches the Databricks workstream's
    # ingest_date Bronze partitioning, and it means a late record never rewrites
    # an old partition.
    prefix              = "${var.bronze_prefix}/ingest_date=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "_errors/${var.bronze_prefix}/!{firehose:error-output-type}/"

    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_seconds

    # No compression_format here: Parquet carries its own (SNAPPY, from the Glue
    # table's parquet.compression property). Setting GZIP alongside format
    # conversion is rejected.
    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        role_arn      = aws_iam_role.firehose.arn
        database_name = var.glue_database_name
        table_name    = var.bronze_table_name
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}
```

Create `infra/modules/native_stream/outputs.tf`:

```hcl
output "stream_name" {
  value = aws_kinesis_stream.trades.name
}

output "stream_arn" {
  value = aws_kinesis_stream.trades.arn
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.bronze.name
}

output "firehose_log_group" {
  value = aws_cloudwatch_log_group.firehose.name
}
```

- [ ] **Step 4: Wire it into the root**

Append to `infra/envs/native/main.tf`:

```hcl
module "stream" {
  source             = "../../modules/native_stream"
  project            = var.project
  region             = data.aws_region.current.name
  account_id         = data.aws_caller_identity.current.account_id
  lake_bucket_arn    = module.lakehouse.bucket_arn
  glue_database_name = module.lakehouse.glue_database_name
  bronze_table_name  = module.lakehouse.bronze_table_name
  bronze_prefix      = module.lakehouse.bronze_prefix
}
```

Append to `infra/envs/native/outputs.tf`:

```hcl
output "stream_name" {
  value = module.stream.stream_name
}

output "firehose_log_group" {
  value = module.stream.firehose_log_group
}
```

- [ ] **Step 5: Apply**

```bash
terraform -chdir=infra/envs/native init
terraform -chdir=infra/envs/native apply
```

Expected: ~7 resources added.

- [ ] **Step 6: Test the whole pipe by hand, before any container exists**

This is the highest-value verification in the plan. Put one record in with the CLI and watch
it come out the other end as Parquet — that tests the stream, the Firehose role, the JSON
deserializer, the Parquet serializer, the Glue schema lookup, and the S3 prefix, without any
of your code involved. If this works, a later failure is your producer; if it doesn't, it's
never your producer.

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

- [ ] **Step 7: Wait for the buffer, then look for the Parquet file**

Firehose will not write until the 120-second interval elapses. Wait, then look:

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
means a missing permission in Step 2's policy.

- [ ] **Step 8: Read it back through Athena**

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

- [ ] **Step 9: Commit**

```bash
git add infra/modules/native_stream/ infra/envs/native/
git commit -m "feat(native): add the stream module -- Kinesis and Firehose

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
producer rather than to the pipe."
```

---

## Task 10: Build and push the producer image

**Concepts.** **ECR** (Elastic Container Registry) is a private Docker registry. Fargate can
only run images it can pull, so the image has to live somewhere AWS-reachable.

The trap, and it will get you: your Mac is **arm64** (Apple Silicon), and Fargate's default
CPU architecture is **x86_64**. An image built without `--platform linux/amd64` pushes fine,
then fails at runtime with `exec format error` — a message that tells you nothing about
architecture. Build for the target explicitly.

A separate `Dockerfile.awsnative` rather than editing the existing one, because the `awsnative`
dependency group is opt-in: the Kafka image must not grow ~50 MB of boto3 it never imports.

**Files:**
- Create: `docker/Dockerfile.awsnative`
- Create: `infra/modules/native_producer/{main.tf,variables.tf,outputs.tf}` (ECR only in this
  task; ECS follows in Task 11)
- Modify: `infra/envs/native/main.tf`, `infra/envs/native/outputs.tf`

**Interfaces:**
- Produces: `module.producer.ecr_repository_url`.

- [ ] **Step 1: Write the Dockerfile**

Create `docker/Dockerfile.awsnative`:

```dockerfile
# The AWS-native producer. Separate from docker/Dockerfile because the
# `awsnative` dependency group is opt-in: the Kafka image must not carry ~50MB
# of boto3 it never imports.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group awsnative

COPY ingest/ ./ingest/
COPY awsnative/ ./awsnative/
COPY config/ ./config/

ENV PATH="/app/.venv/bin:${PATH}"
# No credentials baked in: the task role supplies them at runtime through the
# ECS credential endpoint, and boto3 finds it with no configuration.
CMD ["python", "-m", "awsnative.cli"]
```

- [ ] **Step 2: Add the ECR repository**

```bash
mkdir -p infra/modules/native_producer
```

Create `infra/modules/native_producer/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "stream_arn" {
  type = string
}

variable "stream_name" {
  type = string
}

variable "task_cpu" {
  description = "Fargate CPU units. 256 = 0.25 vCPU. Two WebSocket connections and JSON encoding is not CPU-bound."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "MiB. Must be a valid pairing with task_cpu -- 512 CPU allows 1024-4096."
  type        = number
  default     = 1024
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "desired_count" {
  description = "One. Two tasks would double every trade, since both would consume the same WebSocket streams."
  type        = number
  default     = 1
}
```

Create `infra/modules/native_producer/main.tf`:

```hcl
locals {
  name = "${var.project}-native-producer"
}

resource "aws_ecr_repository" "producer" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# The account is wiped weekly, so images never accumulate for long -- but a
# rebuild loop during development pushes many `latest` layers, and untagged
# parents are pure cost.
resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Expire untagged images after 1 day"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 1
      }
      action = { type = "expire" }
    }]
  })
}
```

Create `infra/modules/native_producer/outputs.tf`:

```hcl
output "ecr_repository_url" {
  value = aws_ecr_repository.producer.repository_url
}

output "ecr_repository_name" {
  value = aws_ecr_repository.producer.name
}
```

- [ ] **Step 3: Wire it into the root and apply**

Append to `infra/envs/native/main.tf`:

```hcl
module "producer" {
  source            = "../../modules/native_producer"
  project           = var.project
  region            = data.aws_region.current.name
  account_id        = data.aws_caller_identity.current.account_id
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.egress_security_group_id
  stream_arn        = module.stream.stream_arn
  stream_name       = module.stream.stream_name
}
```

Append to `infra/envs/native/outputs.tf`:

```hcl
output "ecr_repository_url" {
  value = module.producer.ecr_repository_url
}
```

```bash
terraform -chdir=infra/envs/native init
terraform -chdir=infra/envs/native apply
```

Expected: 2 resources added.

- [ ] **Step 4: Log in to ECR, build for the right architecture, and push**

```bash
REGION=$(terraform -chdir=infra/envs/native output -raw region)
REPO=$(terraform -chdir=infra/envs/native output -raw ecr_repository_url)

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"

# --platform linux/amd64 is not optional on Apple Silicon. Without it the push
# succeeds and the task dies with "exec format error", which says nothing about
# architecture.
docker build --platform linux/amd64 \
  -f docker/Dockerfile.awsnative \
  -t "$REPO:latest" .

docker push "$REPO:latest"
```

Expected: `login succeeded`, a completed build, and a push ending in a `digest: sha256:...`
line.

- [ ] **Step 5: Verify the pushed image's architecture**

```bash
aws ecr describe-images \
  --repository-name "$(terraform -chdir=infra/envs/native output -raw ecr_repository_url | cut -d/ -f2-)" \
  --image-ids imageTag=latest \
  --query 'imageDetails[0].{pushed:imagePushedAt,sizeMB:imageSizeInBytes,digest:imageDigest}'

docker image inspect "$REPO:latest" --format '{{.Architecture}}'
```

Expected: image details, and **`amd64`**. If it says `arm64`, rebuild with `--platform`.

- [ ] **Step 6: Smoke-test the image locally before asking Fargate to run it**

```bash
docker run --rm --platform linux/amd64 "$REPO:latest" \
  python -c "import awsnative.cli, boto3; print('imports ok', boto3.__version__)"
```

Expected: `imports ok 1.3x.x`. This catches a missing `COPY` or dependency group in seconds,
where the same mistake in Fargate costs a log-hunting round trip.

- [ ] **Step 7: Commit**

```bash
git add docker/Dockerfile.awsnative infra/modules/native_producer/ infra/envs/native/
git commit -m "feat(native): add the producer image and ECR repository

A separate Dockerfile rather than editing docker/Dockerfile: the awsnative
dependency group is opt-in, and the Kafka image must not carry ~50MB of boto3
it never imports.

No credentials in the image -- the ECS task role supplies them at runtime
through the container credential endpoint, which boto3 finds unconfigured.

Build must pass --platform linux/amd64: an arm64 image from Apple Silicon
pushes cleanly and then dies in Fargate with 'exec format error', a message
that names nothing about architecture."
```

---

## Task 11: Run the producer on Fargate

**Concepts.** **ECS** (Elastic Container Service) is an orchestrator. **Fargate** is its
serverless mode: you describe a container, AWS finds the compute. Three pieces:

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
WebSocket connections and publish every trade twice. Deduplication would absorb it downstream,
but doubling the bill to create work for the dedupe is not a feature.

**Files:**
- Modify: `infra/modules/native_producer/main.tf` (append), `outputs.tf` (append)

**Interfaces:**
- Consumes: `var.{subnet_ids,security_group_id,stream_arn,stream_name,task_cpu,task_memory,image_tag,desired_count}`.
- Produces: `module.producer.{cluster_name,service_name,log_group}`.

- [ ] **Step 1: Add the two IAM roles**

Append to `infra/modules/native_producer/main.tf`:

```hcl
# --- Roles -----------------------------------------------------------------
# Two roles, two actors. The execution role is used by the ECS agent BEFORE the
# container starts (pull the image, create the log stream). The task role is
# used by the running code. Different lifetimes, different permissions.

data "aws_iam_policy_document" "ecs_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
}

# AWS's managed policy covers exactly ECR pull + CloudWatch Logs write.
resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
}

data "aws_iam_policy_document" "task_permissions" {
  # Least privilege: write to one stream, and nothing else. No read actions --
  # the producer never consumes.
  statement {
    effect  = "Allow"
    actions = ["kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStreamSummary"]
    resources = [var.stream_arn]
  }

  # The stream is KMS-encrypted, so producing needs a data key.
  statement {
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey"]
    resources = ["arn:aws:kms:${var.region}:${var.account_id}:alias/aws/kinesis"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_permissions.json
}
```

- [ ] **Step 2: Add the cluster, log group, task definition, and service**

Append to `infra/modules/native_producer/main.tf`:

```hcl
# --- Compute ---------------------------------------------------------------

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${local.name}"
  retention_in_days = 7
}

resource "aws_ecs_cluster" "this" {
  name = "${var.project}-native"

  setting {
    name  = "containerInsights"
    value = "disabled" # billed per metric; the log stream is enough at one task
  }
}

resource "aws_ecs_task_definition" "producer" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    # Matches docker build --platform linux/amd64. If these disagree the task
    # starts and dies with "exec format error".
    cpu_architecture = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "producer"
    image     = "${aws_ecr_repository.producer.repository_url}:${var.image_tag}"
    essential = true

    environment = [
      { name = "FDAI_NATIVE_STREAM_NAME", value = var.stream_name },
      { name = "AWS_REGION", value = var.region },
      # boto3's default retry mode gives up sooner than KinesisSink's own
      # backoff; standard mode defers to ours rather than fighting it.
      { name = "AWS_RETRY_MODE", value = "standard" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.producer.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "producer"
      }
    }
  }])
}

# desired_count = 1 deliberately. Two tasks would each open their own Binance
# and Coinbase connections and publish every trade twice; dedupe downstream
# would absorb it, but doubling the bill to create work for the dedupe is not a
# feature.
resource "aws_ecs_service" "producer" {
  name            = local.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets = var.subnet_ids
    security_groups = [var.security_group_id]
    # Public IP instead of a NAT gateway: the task needs outbound WSS and
    # nothing inbound, and a NAT would cost more per month than this whole
    # stack (spec section 4.4).
    assign_public_ip = true
  }

  # A rolling replacement of a single task briefly runs two, which double-writes
  # for a few seconds. Stopping the old one first accepts a small gap instead --
  # and a gap is detectable and repairable, where a duplicate is a silent volume
  # error until dedupe runs.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  enable_execute_command = true # lets you shell into the task to debug
}
```

Append to `infra/modules/native_producer/outputs.tf`:

```hcl
output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "service_name" {
  value = aws_ecs_service.producer.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.producer.name
}
```

- [ ] **Step 3: Export the new outputs from the root**

Append to `infra/envs/native/outputs.tf`:

```hcl
output "ecs_cluster" {
  value = module.producer.cluster_name
}

output "ecs_service" {
  value = module.producer.service_name
}

output "producer_log_group" {
  value = module.producer.log_group
}
```

- [ ] **Step 4: Apply**

```bash
terraform -chdir=infra/envs/native apply
```

Expected: ~9 resources added.

- [ ] **Step 5: Watch the task reach RUNNING**

```bash
CLUSTER=$(terraform -chdir=infra/envs/native output -raw ecs_cluster)
SERVICE=$(terraform -chdir=infra/envs/native output -raw ecs_service)

aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
  --query 'services[0].{desired:desiredCount,running:runningCount,pending:pendingCount}'
```

Expected, within a minute or two: `running: 1, pending: 0`.

If `running` stays 0, the reason is almost always in the service events:

```bash
aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
  --query 'services[0].events[0:5].message' --output text
```

- [ ] **Step 6: Read the producer's logs**

```bash
aws logs tail "$(terraform -chdir=infra/envs/native output -raw producer_log_group)" --since 5m --follow
```

Expected: JSON lines including `starting_venue` for both `binance` and `coinbase` with a
`stream` field, then quiet (the runner logs on events, not per trade). Press Ctrl-C to stop.

Common failures and what they mean:

| Log or event | Cause |
|---|---|
| `exec format error` | arm64 image. Rebuild with `--platform linux/amd64` (Task 10 Step 4). |
| `AccessDeniedException ... kinesis:PutRecords` | Task role policy missing or wrong stream ARN. |
| `CannotPullContainerError` | Execution role or no route to ECR — check `assign_public_ip`. |
| `ValidationError: no venues to run` | `config/` did not make it into the image. |
| Nothing at all in the log group | Execution role can't create the stream; check the managed policy attachment. |

- [ ] **Step 7: Confirm records are actually arriving in Kinesis**

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

- [ ] **Step 8: Commit**

```bash
git add infra/modules/native_producer/ infra/envs/native/
git commit -m "feat(native): run the producer on ECS Fargate

Two roles because there are two actors: the execution role is used by the ECS
agent before the container starts (ECR pull, log stream), the task role by the
running code (kinesis:PutRecords on one stream, nothing else). boto3 finds the
task role through the container credential endpoint, so no key is configured
anywhere.

desired_count = 1 deliberately -- two tasks would each open their own exchange
connections and publish every trade twice. Deployment is stop-then-start rather
than rolling for the same reason: a brief gap is detectable and repairable,
where a brief duplicate is a silent volume error until dedupe runs.

runtime_platform pins X86_64 to match the image build; a mismatch here presents
as 'exec format error'."
```

---

## Task 12: End-to-end verification in Athena

**Concepts.** Nothing new — this task is the gate. Everything so far has been verified
component by component; this proves the composed system produces *correct* data, not merely
data. The queries below check three separate things: that rows arrive, that no column is
systematically null (the classic symptom of a schema/encoder mismatch that Firehose
tolerates), and that both venues are present (the classic symptom of one connector silently
failing).

**Files:**
- Create: `awsnative/sql/verify_bronze.sql`

- [ ] **Step 1: Let it run for at least five minutes**

Firehose buffers for 120 s, so anything less than ~3 minutes tells you nothing. Five gives you
two or three files and enough rows for the null checks to mean something.

- [ ] **Step 2: Save the verification queries**

```bash
mkdir -p awsnative/sql
```

Create `awsnative/sql/verify_bronze.sql`:

```sql
-- Stage N1 acceptance queries. Run each and compare against the expectations in
-- the plan; they check three different failure modes, so run all three.

-- 1. Rows are arriving, and from both venues.
--    A missing venue means one connector failed silently -- the runner logs on
--    reconnect, not on "never connected".
SELECT venue,
       count(*)                AS rows,
       count(DISTINCT trade_id) AS distinct_trades,
       min(from_unixtime(event_ts_us / 1000000)) AS first_event,
       max(from_unixtime(event_ts_us / 1000000)) AS last_event
FROM bronze_trades_stream
GROUP BY venue
ORDER BY venue;

-- 2. No column is systematically null.
--    Firehose's OpenX deserializer silently writes NULL for a JSON key that does
--    not match a Glue column, so a rename shows up here and nowhere else.
SELECT count(*)                                         AS rows,
       count_if(venue IS NULL)                          AS null_venue,
       count_if(instrument_id IS NULL)                  AS null_instrument,
       count_if(trade_id IS NULL)                       AS null_trade_id,
       count_if(event_ts_us IS NULL)                    AS null_event_ts,
       count_if(price IS NULL)                          AS null_price,
       count_if("size" IS NULL)                         AS null_size,
       count_if(side IS NULL)                           AS null_side,
       count_if(source IS NULL)                         AS null_source,
       count_if(schema_version IS NULL)                 AS null_schema_version
FROM bronze_trades_stream;

-- 3. The values are sane, not merely present.
--    Zero or negative prices, or timestamps outside a plausible epoch range,
--    mean the encoder or a parser is wrong even though the pipe works.
SELECT count_if(CAST(price AS DECIMAL(38, 18)) <= 0)   AS nonpositive_price,
       count_if(CAST("size" AS DECIMAL(38, 18)) <= 0)  AS nonpositive_size,
       count_if(side NOT IN ('BUY', 'SELL', 'UNKNOWN')) AS bad_side,
       count_if(event_ts_us < 1500000000000000)         AS ts_too_old,
       count_if(event_ts_us > 1900000000000000)         AS ts_too_new,
       count_if(schema_version <> 1)                    AS wrong_schema_version
FROM bronze_trades_stream;
```

- [ ] **Step 3: Run query 1 — rows from both venues**

Use the Athena console for this one (Query editor, workgroup `fdai-native`, database
`fdai_native`) — reading a result table beats parsing CLI JSON, and you should see the
console's "Data scanned" figure, which is what you're billed on.

Expected: two rows, `binance` and `coinbase`, both with non-zero `rows`, `distinct_trades`
close to `rows`, and `last_event` within the last couple of minutes.

If `coinbase` is missing, check the producer log for that venue — the Coinbase connector needs
a subscribe payload accepted before it emits anything.

- [ ] **Step 4: Run query 2 — no systematic nulls**

Expected: `rows` > 0 and **every** `null_*` column exactly `0`.

Any non-zero null count except `null_sequence` (which is legitimately nullable for Coinbase)
means the Glue column name and the JSON key disagree. Compare
`infra/modules/native_lakehouse/main.tf`'s `columns` blocks against `awsnative/encode.py`.
This is the failure the Task 5 contract test cannot catch, because the Glue table is a third
party to that agreement.

- [ ] **Step 5: Run query 3 — values are sane**

Expected: every column `0`.

- [ ] **Step 6: Record the acceptance result**

Add the numbers you got to the commit message, so the stage has evidence attached rather than
a claim.

```bash
git add awsnative/sql/verify_bronze.sql
git commit -m "test(native): add the stage N1 acceptance queries

Three queries checking three different failure modes: rows from both venues
(a missing venue means a connector failed silently), no systematic nulls
(Firehose's OpenX deserializer writes NULL for a JSON key that does not match
a Glue column, so a rename shows up here and nowhere else), and sane values
(a working pipe carrying wrong numbers).

The null check is the one the encoder contract test cannot cover, because the
Glue table is a third party to the encoder/avsc agreement."
```

---

## Task 13: `make up-aws` / `make down-aws`

**Concepts.** The reproducibility contract from the parent spec: *any manual console step is
a bug.* You have been running steps by hand to learn the services, which was the point — this
task collapses them into one ordered command so the next weekly rebuild is a single
invocation.

Ordering matters and is not obvious. Terraform can create the ECS service before an image
exists in ECR; the service then sits retrying `CannotPullContainerError` until an image
appears. So: apply infrastructure, push the image, *then* force the service to redeploy.

**Files:**
- Create: `scripts/native_up.sh`
- Modify: `Makefile`, `README.md`

- [ ] **Step 1: Write the bring-up script**

Create `scripts/native_up.sh`:

```bash
#!/usr/bin/env bash
# Empty AWS account -> streaming trades in Bronze, in one command.
#
# Order is load-bearing: Terraform will happily create the ECS service before
# any image exists in ECR, and the service then sits retrying
# CannotPullContainerError. So infrastructure first, image second, redeploy
# third.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/infra/envs/native"

cd "$ROOT"

printf '==> preflight\n'
./scripts/native_preflight.sh

printf '\n==> rendering the backend config\n'
ACCT=$(aws sts get-caller-identity --query Account --output text)
REGION="${AWS_REGION:-$(aws configure get region)}"
sed -e "s|\${state_bucket}|fdai-tfstate-$ACCT|" \
    -e "s|\${region}|$REGION|" \
    -e "s|\${lock_table}|fdai-tflock|" \
    "$DIR/backend.tf.tftpl" > "$DIR/backend.tf"

printf '\n==> terraform apply\n'
terraform -chdir="$DIR" init -input=false
terraform -chdir="$DIR" apply -auto-approve

printf '\n==> building and pushing the producer image\n'
REPO=$(terraform -chdir="$DIR" output -raw ecr_repository_url)
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"
# --platform is not optional: an arm64 image pushes fine and then dies in
# Fargate with "exec format error".
docker build --platform linux/amd64 -f docker/Dockerfile.awsnative -t "$REPO:latest" .
docker push "$REPO:latest"

printf '\n==> forcing a new deployment so the service picks up the image\n'
CLUSTER=$(terraform -chdir="$DIR" output -raw ecs_cluster)
SERVICE=$(terraform -chdir="$DIR" output -raw ecs_service)
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
  --force-new-deployment >/dev/null
aws ecs wait services-stable --cluster "$CLUSTER" --service "$SERVICE"

printf '\n==> up.\n'
printf 'Producer logs : aws logs tail %s --follow\n' \
  "$(terraform -chdir="$DIR" output -raw producer_log_group)"
printf 'Bronze lands in ~2 min (Firehose buffers for 120s).\n'
printf 'Acceptance queries: awsnative/sql/verify_bronze.sql\n'
```

```bash
chmod +x scripts/native_up.sh
```

- [ ] **Step 2: Add the Makefile targets**

Append to `Makefile`:

```make
.PHONY: up-aws down-aws rebuild-aws preflight-aws logs-aws validate-aws

NATIVE := infra/envs/native

preflight-aws:
	./scripts/native_preflight.sh

# Offline: -backend=false skips the S3 backend, so this needs no AWS
# credentials and can run in CI on a pull request. Deliberately not folded into
# `make check`, which must stay runnable with no cloud tooling installed at all.
validate-aws:
	terraform -chdir=$(NATIVE) init -backend=false -input=false >/dev/null
	terraform -chdir=$(NATIVE) validate
	terraform -chdir=$(NATIVE) fmt -check -recursive ../../modules
	@command -v tflint  >/dev/null && tflint --chdir=$(NATIVE) || echo "tflint not installed, skipped"
	@command -v checkov >/dev/null && checkov -d $(NATIVE) --quiet --compact || echo "checkov not installed, skipped"

up-aws:
	./scripts/native_up.sh

# force_destroy / force_delete are set on the lake bucket and the ECR repo, so
# a non-empty bucket or a repo with images does not block teardown. Everything
# here is re-derivable (spec section 6), which is what makes that safe.
down-aws:
	terraform -chdir=$(NATIVE) destroy -auto-approve

rebuild-aws: down-aws up-aws

logs-aws:
	aws logs tail "$$(terraform -chdir=$(NATIVE) output -raw producer_log_group)" --follow
```

- [ ] **Step 3: Prove the contract — destroy and rebuild from empty**

This is the acceptance test for the whole stage. Anything you created by hand and forgot to
put in Terraform disappears here and does not come back.

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

- [ ] **Step 4: Re-run the acceptance queries against the rebuilt stack**

Wait five minutes, then run all three queries from `awsnative/sql/verify_bronze.sql` again.

Expected: the same shape of results as Task 12 — both venues, no nulls, sane values. That the
data is *new* is the point: nothing was restored, it was re-derived from the live stream.

- [ ] **Step 5: Document it in the README**

Add to `README.md`, after the existing "The reproducibility contract" section:

```markdown
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
```

- [ ] **Step 6: Final check and commit**

```bash
make check
make validate-aws
```

Expected: `make check` green, and `validate-aws` printing `Success! The configuration is
valid.` `tflint` and `checkov` are optional — if they're not installed the target says so and
carries on rather than failing. Install them (`brew install tflint`, `uv tool install checkov`)
when you want the extra pass; `checkov` will flag the public-subnet task and the unrestricted
egress SG, both of which are deliberate and documented in spec §4.4.

```bash
git add scripts/native_up.sh Makefile README.md
git commit -m "feat(native): add make up-aws / down-aws

Collapses the hand-run stage N0-N1 steps into one ordered command. The order
is load-bearing: Terraform will create the ECS service before any image exists
in ECR and the service then sits retrying CannotPullContainerError, so it is
apply, then push, then force-new-deployment.

Verified by destroying the stack and rebuilding from empty, then re-running the
acceptance queries -- which is also the test that nothing was created by hand
and left out of Terraform."
```

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
