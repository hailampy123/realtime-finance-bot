# Documentation map

Start with the question you have, not with a file name.

| Your question | Read |
|---|---|
| What is this project? | [`../README.md`](../README.md) |
| Which commands bring it up? | [`../README.md`](../README.md), then [`SETUP.md`](SETUP.md) |
| What is built, and what is only designed? | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Where does the data live, and what is one row? | [`DATA_LAYER.md`](DATA_LAYER.md) |
| When does data actually get written, and how fresh is it? | [`DATA_LAYER.md`](DATA_LAYER.md) §6, §7 |
| Which file do I change for X? | [`CODEBASE_EXPLAINED.md`](CODEBASE_EXPLAINED.md) |
| It broke after a deploy. | [`AWS_DEPLOYMENT_DEBUGGING.md`](AWS_DEPLOYMENT_DEBUGGING.md) |
| What is Kafka / MSK / a security group? | the three primers below |
| What should we build next? | [`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md) |
| Why was it built this way? | [`superpowers/specs/`](superpowers/specs/) |

---

## The four kinds of document here

Each document does one job. When you add content, put it in the document whose
job matches, and cross-link instead of repeating.

### 1. Current state: what exists today

These describe the repo and the accounts as they are. If a document here
disagrees with the code, the document is wrong.

| Document | Job | Not its job |
|---|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Diagrams of both stacks: what moves data, what runs where, what is built versus designed | Table schemas, install steps |
| [`DATA_LAYER.md`](DATA_LAYER.md) | Every stored table in both workstreams: location, format, partitioning, grain, key, lineage, scheduling, freshness, estimated size | Diagrams, roadmap |
| [`CODEBASE_EXPLAINED.md`](CODEBASE_EXPLAINED.md) | Directory tour, module boundaries, where a given change belongs | AWS resources, table schemas |

### 2. Operate: the commands you run

| Document | Job | Not its job |
|---|---|---|
| [`SETUP.md`](SETUP.md) | Prerequisites, the manual account steps Terraform cannot do, every configuration variable, and the first-run walkthrough | Explaining what a service is |
| [`AWS_DEPLOYMENT_DEBUGGING.md`](AWS_DEPLOYMENT_DEBUGGING.md) | Verification and diagnosis after a deploy: the checks, in order, and the failure guide | Install steps |
| [`RUNBOOK_STAGE_2A.md`](RUNBOOK_STAGE_2A.md) | Deploying and validating the Databricks Bronze/Silver pipeline | The AWS-native stack |
| [`../notebooks/README.md`](../notebooks/README.md) | The local notebook loop against either broker | Production behavior |

### 3. Primers: concepts, for someone new to them

Written for a reader who is fluent in Python and new to Terraform, AWS, and
Kafka. Every explanation is grounded in this repo's own configuration rather
than in a generic example.

| Document | Job |
|---|---|
| [`AWS_SERVICES_EXPLAINED.md`](AWS_SERVICES_EXPLAINED.md) | What each AWS service is, how ARNs and regions work, and the inspection command for each |
| [`KAFKA_EXPLAINED.md`](KAFKA_EXPLAINED.md) | Topics, partitions, keys, retention, consumer groups, and the settings this project chose |
| [`MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md) | Resource by resource, what `make up` creates and why, plus cost |

### 4. Design history: why, and what is planned

| Path | Job |
|---|---|
| [`superpowers/specs/`](superpowers/specs/) | The design of each slice, with its rejected alternatives and its open assumptions. Written before the code. |
| [`superpowers/plans/`](superpowers/plans/) | The task-by-task run guide for each slice, including the manual verification steps |
| [`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md) | The roadmap: what to add next, in order, with the reason for each position |

Specs and plans are dated and frozen. They record intent at the time of
writing. When the implementation diverges, the spec's deviations section records
it and [`DATA_LAYER.md`](DATA_LAYER.md) §12 summarizes it. Do not read a spec as
a description of current state.

---

## Stage names

Both workstreams number their slices. The names appear throughout the specs and
plans.

| Stage | Scope | State |
|---|---|---|
| Stage 1 | Kafka ingestion: connectors, MSK, producer host | built |
| Stage 2a | Databricks Bronze and Silver | built, not run live |
| N0–N1 | AWS-native ingestion: Fargate, Kinesis, Firehose, Bronze | built |
| N2–N3 | AWS-native Silver, quarantine, and Gold bars | built |
| E1, E3 | Perpetual-futures context and vintage-stamped macro | built |
| N4 | Archive backfill and reconciliation | designed |
| N5 | Point-in-time serving boundary | designed |
| N6 | Agent | designed |
| E2, E4 | Liquidity from order-book depth; narrative | proposed |

## Conventions

- A document in "current state" that mentions an unbuilt component says so on
  the same line.
- Diagrams mark built components in solid green and designed ones in dashed
  gray.
- Commands are `make` targets wherever one exists. A raw `aws` or `terraform`
  command in a document means no target covers it.
- Section references like §5.4 point at the design spec named in the same
  paragraph.
