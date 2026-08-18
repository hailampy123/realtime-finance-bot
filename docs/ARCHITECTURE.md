# Architecture: current state

What exists in this repo today, drawn from the source files cited under each
diagram. Solid green means built and live after the bring-up command. Dashed
gray means designed with no code yet. If a box is not green, no `make` target
creates it.

This document holds the topology. Table schemas and column contracts live in
[`DATA_LAYER.md`](DATA_LAYER.md). Install steps live in [`SETUP.md`](SETUP.md).
The target end state lives in the design specs, which these diagrams
deliberately do not show:
[platform](superpowers/specs/2026-08-07-finance-data-ai-platform-design.md),
[lakehouse](superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md),
[AWS-native](superpowers/specs/2026-08-14-aws-native-workstream-design.md).

---

## 1. Two stacks, one ingestion contract

The repo implements the same use case twice. Both stacks run the same Python
connectors against the same exchange feeds, then diverge at the sink.

| Property | Stack A: Kafka / Databricks | Stack B: AWS-native |
|---|---|---|
| Bring up | `make up` | `make up-aws` |
| Producer runs on | EC2 `t3.small`, Docker | ECS Fargate, 0.5 vCPU |
| Transport | MSK, SASL/SCRAM | Kinesis Data Streams, on-demand |
| Lands as | Kafka topic `md.trades.v1` | Parquet on S3, via Firehose |
| Transforms | PySpark in a Lakeflow pipeline | Athena SQL, driven by Step Functions |
| Durable store | Unity Catalog Delta, permanent account | nothing; re-derivation from public archives |
| VPC | `10.42.0.0/16` | `10.43.0.0/16` |
| Terraform env | `infra/envs/dev` | `infra/envs/native` |
| Bring-up time | 45 to 60 minutes | a few minutes |

Both can run at once. The CIDRs do not overlap, the state keys differ, and every
Stack B resource name carries the `fdai-native-` prefix.

## 2. Shared ingestion: the code above both sinks

Everything in this section ships in both stacks. `ingest/` holds no AWS-specific
and no Databricks-specific import.

```mermaid
flowchart LR
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5

    BinanceWS["Binance WS\nwss://.../@aggTrade"]:::built
    CoinbaseWS["Coinbase WS\nmarket_trades channel"]:::built
    KrakenWS["Kraken WS"]:::planned
    AlpacaWS["Alpaca IEX WS"]:::planned
    NewsWS["Alpaca news WS /\nFinnhub REST poll"]:::planned

    BinanceConn["BinanceConnector\ningest/connectors/binance.py"]:::built
    CoinbaseConn["CoinbaseConnector\ningest/connectors/coinbase.py"]:::built
    NoConn["no connector code yet"]:::planned

    BinanceWS --> BinanceConn
    CoinbaseWS --> CoinbaseConn
    KrakenWS -.-> NoConn
    AlpacaWS -.-> NoConn
    NewsWS -.-> NoConn

    Queue["BoundedTopicQueue\nper-topic backpressure\ningest/core/queue.py"]:::built
    Runner["IngestRunner\ngap detection + REST repair\ningest/runner.py"]:::built

    BinanceConn --> Queue
    CoinbaseConn --> Queue
    Queue --> Runner
    Runner -. REST gap repair .-> BinanceConn
    Runner -. REST gap repair .-> CoinbaseConn

    SinkA["TradeProducer\nbare Avro + SASL/SCRAM\ningest/core/producer.py"]:::built
    SinkB["KinesisSink\nJSON lines\nawsnative/sink.py"]:::built

    Runner --> SinkA
    Runner --> SinkB

    SinkA --> StackA["Stack A: MSK"]:::built
    SinkB --> StackB["Stack B: Kinesis"]:::built
```

Two connectors exist, Binance and Coinbase. Both emit trades and nothing else,
because no producer code exists for the other topics.

A queue sits between parsing and publishing because publishing must never block
frame consumption. A slow write would stall the WebSocket read buffer and the
exchange would disconnect. `md.trades.v1` blocks when the queue fills;
`md.book.*` would drop instead, because the next depth snapshot recovers a lost
update and nothing recovers a lost trade short of a REST call.

## 3. Stack A: Kafka and Databricks

### 3.1 What moves data

```mermaid
flowchart LR
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5

    Producer["TradeProducer\non the EC2 host"]:::built

    subgraph MSK["MSK topics (scripts/create_topics.py)"]
        Trades["md.trades.v1\npopulated"]:::built
        Others["md.book.top.v1, md.book.depth.v1,\nmd.bars.v1, news.articles.v1,\nops.metrics.v1, _dlq.*\nprovisioned, empty"]:::planned
    end

    Producer --> Trades
    Producer -.-> Others

    Bronze["bronze_trades_stream\nDelta streaming table"]:::built
    Validated["trades_validated\ntemporary view + expectations"]:::built
    Silver["silver_trades\nAUTO CDC, SCD Type 1"]:::built
    Quar["silver_trades_quarantine"]:::built
    Gold["gold (design only)"]:::planned

    Trades --> Bronze --> Validated
    Validated --> Silver
    Validated --> Quar
    Silver -.-> Gold

    ConsumeExample["scripts/consume_example.py\nmanual, ad-hoc VWAP"]:::built
    Trades --> ConsumeExample

    Archive["data.binance.vision\narchives"]:::planned
    Archive -.-> Bronze
```

The pipeline is triggered, not continuous. Nothing downstream of Silver consumes
it yet, so an always-on cluster would spend money to feed nobody. Deploying and
running it is [`RUNBOOK_STAGE_2A.md`](RUNBOOK_STAGE_2A.md).

### 3.2 Where it runs

```mermaid
flowchart TB
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5
    classDef acct fill:#eef2ff,stroke:#4c51bf,color:#2a2f6b,stroke-width:2px

    GitHub["GitHub\nhailampy123/realtime-finance-bot\npublic repo"]:::built

    subgraph AcctA["ACCOUNT A: sandbox, wiped every 7 days"]
        direction TB

        subgraph Bootstrap["State backend (infra/bootstrap), local Terraform state"]
            S3["S3 bucket\nfdai-tfstate-<account-id>\nversioned, AES256\npublic access blocked"]:::built
            DDB["DynamoDB\nfdai-tflock"]:::built
        end

        subgraph VPC["VPC 10.42.0.0/16 (infra/modules/network)"]
            IGW["Internet Gateway"]:::built
            SubnetA["Public subnet AZ-0"]:::built
            SubnetB["Public subnet AZ-1"]:::built
            SGProducer["SG: producer\negress + operator SSH /32"]:::built
            SGMsk["SG: msk\n9196 from kafka_client_cidrs\n9096 from VPC only"]:::built

            EC2["EC2 t3.small (AL2023)\nno instance profile\nimage built on the host\ninfra/modules/producer_host"]:::built
            MSKCluster["MSK cluster: fdai-kafka\n2x kafka.t3.small, 50GB EBS each\nSASL/SCRAM only, TLS in-cluster\ninfra/modules/kafka_msk"]:::built
            MSKConfig["MSK configuration\nallow.everyone.if.no.acl.found\nfalse once ACLs exist"]:::built

            MSKConfig -. attached to .-> MSKCluster

            SubnetA --- EC2
            SubnetA --- MSKCluster
            SubnetB --- MSKCluster
            IGW --- SubnetA
            IGW --- SubnetB
            SGProducer -. applies to .-> EC2
            SGMsk -. applies to .-> MSKCluster
        end

        Secret["Secrets Manager\nAmazonMSK_fdai_producer\nSCRAM username + password"]:::built
        KMS["Customer-managed KMS key\nalias/fdai-msk-scram"]:::built
        Policy["Resource policy:\nPrincipal = kafka.amazonaws.com,\nnot an IAM role"]:::built

        KMS --> Secret
        Policy -. grants read .-> Secret
        Secret -->|aws_msk_scram_secret_association| MSKCluster
        GitHub -->|"git clone, unauthenticated,\nno instance profile needed"| EC2
        EC2 -->|SASL/SCRAM produce| MSKCluster
    end

    subgraph AcctB["ACCOUNT B: Databricks workspace, permanent\nyou hold workspace admin"]
        direction TB
        NAT["NAT Gateway\nEIP allowlisted in SGMsk"]:::built
        SecretScope["Databricks secret scope 'fdai'\nkafka_bootstrap / kafka_username /\nkafka_password\nrepublished on every make up"]:::built
        UC["Unity Catalog fdai.market\nbronze_trades_stream, silver_trades,\nsilver_trades_quarantine"]:::built
    end

    Operator["Your machine\nscripts/bootstrap.sh"]:::built
    Operator -->|"SSH 22, operator /32 only:\nKafka ACL bootstrap\n(scripts/create_acls.py)"| EC2

    MSKCluster -->|"public bootstrap endpoint\nSASL_SSL:9196"| NAT
    NAT --> UC
    Secret -->|"databricks CLI, run\nfrom your machine\n(scripts/bootstrap.sh)"| SecretScope

    class AcctA,AcctB acct
```

Terraform creates and destroys every solid box in Account A. The only thing that
outlives a run is the S3 state bucket, which sits in the same account and dies
in the same wipe. State and reality stay consistent because they disappear
together.

One link crosses into Account B: the MSK public bootstrap endpoint, reachable
only from the CIDRs in `kafka_client_cidrs`. That list holds the Databricks NAT
Elastic IP and your own address. There is no IAM role, no VPC peering, and no
PrivateLink.

The SSH arrow needs an explanation. MSK refuses to enable public access while
`allow.everyone.if.no.acl.found` is true. Setting it false makes Kafka
deny-by-default, so ACLs must already exist. Writing an ACL needs a reachable
broker, and before public access the only reachable place is inside the VPC. The
producer host is the only thing in there. Hence a per-run keypair and a `/32`
SSH rule that exist for one step of `make up`. Full reasoning:
[`SETUP.md`](SETUP.md) §5f.

Resource by resource, with cost: [`MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md).

## 4. Stack B: AWS-native

### 4.1 What moves data

```mermaid
flowchart LR
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5

    Sink["KinesisSink\nawsnative/sink.py"]:::built
    KDS["Kinesis Data Streams\nfdai-native-trades\non-demand, 24h retention"]:::built
    FH["Firehose\nJSON to Parquet,\n128 MB or 120 s"]:::built
    BronzeT["bronze_trades_stream\nParquet, ingest_date"]:::built

    Sink --> KDS --> FH --> BronzeT

    PerpL["perp_handler Lambda\nrate(5 minutes)"]:::built
    MacroL["macro_handler Lambda\ncron(30 6 * * ? *)"]:::built
    BronzeP["bronze_perp_context\nJSON, ingest_date"]:::built
    BronzeM["bronze_macro_observations\nJSON, ingest_date"]:::built

    BinanceREST["Binance REST\nfunding, OI, positioning"]:::built
    ALFRED["ALFRED CSV\n6 macro series"]:::built

    BinanceREST --> PerpL --> BronzeP
    ALFRED --> MacroL --> BronzeM

    SilverT["silver_trades"]:::built
    Quar["silver_trades_quarantine"]:::built
    SilverP["silver_perp_context"]:::built
    SilverM["silver_macro"]:::built
    GoldB["gold_bars_1m"]:::built

    BronzeT --> SilverT
    BronzeT --> Quar
    BronzeP --> SilverP
    BronzeM --> SilverM
    SilverT --> GoldB

    Archive["data.binance.vision\nklines + aggTrades"]:::planned
    Staging["archive_staging_*\n+ backfill_manifest"]:::planned
    Archive -.-> Staging
    Staging -.-> SilverT
    Staging -.-> GoldB

    PIT["N5: *_pit statements,\ntool-server IAM scoping"]:::planned
    Agent["N6: agent"]:::planned
    GoldB -.-> PIT -.-> Agent

    Dash["awsnative/dashboard\nstatic HTML"]:::built
    GoldB --> Dash
    SilverP --> Dash
    SilverM --> Dash
```

Backfill code exists in [`awsnative/backfill/`](../awsnative/backfill/) and its
tables have DDL, but no scheduled process runs it. Until stage N4 lands, Silver
and Gold hold only what has streamed in since the last bring-up.

### 4.2 Where it runs

```mermaid
flowchart TB
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5
    classDef acct fill:#eef2ff,stroke:#4c51bf,color:#2a2f6b,stroke-width:2px

    subgraph AcctA["ACCOUNT A: the same sandbox, wiped every 7 days"]
        direction TB

        subgraph Net["VPC 10.43.0.0/16 (infra/modules/native_network)"]
            IGW["Internet Gateway"]:::built
            Sub["2 public subnets,\nmap_public_ip_on_launch"]:::built
            SG["SG: fdai-native-egress\negress only, no ingress"]:::built
            Task["ECS Fargate task\n512 CPU / 1024 MiB, desired_count 1\nassign_public_ip\ninfra/modules/native_producer"]:::built
            IGW --- Sub --- Task
            SG -. applies to .-> Task
        end

        ECR["ECR: fdai-native-producer\nuntagged images expire in 1 day"]:::built
        ECR -->|image pull| Task

        KDS["Kinesis: fdai-native-trades\nON_DEMAND, 24h\ninfra/modules/native_stream"]:::built
        FH["Firehose: fdai-native-bronze\nGlue-driven Parquet conversion"]:::built
        Task -->|PutRecords, task role| KDS --> FH

        subgraph Lake["S3: fdai-native-lake-<account-id> (infra/modules/native_lakehouse)"]
            BronzePrefix["bronze_*/ingest_date=.../"]:::built
            IcebergPrefix["silver_*/, gold_bars_1m/,\nbackfill_manifest/"]:::built
            Results["_athena-results/, _errors/\nlifecycle expiry"]:::built
        end

        FH --> BronzePrefix

        Glue["Glue database fdai_native\nBronze via partition projection,\nIceberg via Athena DDL"]:::built
        WG["Athena workgroup fdai-native\nenforce_workgroup_configuration"]:::built

        BronzePrefix -.-> Glue
        IcebergPrefix -.-> Glue
        Glue --- WG
        WG --> Results

        SFN["Step Functions\nfdai-native-microbatch\ninfra/modules/native_medallion"]:::built
        Sched["EventBridge Scheduler\nrate(5 minutes), armed by\nmicrobatch_enabled"]:::built
        Sched -->|states:StartExecution| SFN
        SFN -->|"startQueryExecution.sync"| WG
        WG --> IcebergPrefix

        PerpFn["Lambda perp collector\n120s, 256 MB"]:::built
        MacroFn["Lambda macro collector\n120s, 256 MB"]:::built
        Sched2["EventBridge Scheduler x2\narmed by enrichment_enabled"]:::built
        Sched2 --> PerpFn --> BronzePrefix
        Sched2 --> MacroFn --> BronzePrefix

        Budget["Budgets: fdai-native-monthly\nemail at 80% of limit"]:::built
        Logs["CloudWatch Logs\nproducer, Firehose, state machine,\nboth Lambdas"]:::built
    end

    Binance["Binance WS + REST"]:::built
    Coinbase["Coinbase WS"]:::built
    ALFRED["ALFRED CSV"]:::built
    Binance --> Task
    Coinbase --> Task
    Binance --> PerpFn
    ALFRED --> MacroFn

    Operator["Your machine\nscripts/native_up.sh"]:::built
    Operator -->|"terraform apply, docker build + push,\nmake ddl-aws"| ECR

    class AcctA acct
```

Stack B uses IAM roles where Stack A cannot: a Fargate execution role, a task
role scoped to `PutRecords` on one stream, a Firehose delivery role, a state
machine role, and a scheduler role that can do nothing except start that one
execution. The sandbox permits creating roles; it forbids nothing this stack
needs. Run `make preflight-aws` to confirm that before spending 10 minutes on an
apply.

Lambda arrives only in the enrichment collectors. The producer runs as a
long-lived Fargate task, and the transforms run as Athena statements with no
code deployed at all.

### 4.3 The micro-batch

One Step Functions state machine runs the whole medallion. Definition:
[`infra/modules/native_medallion/main.tf`](../infra/modules/native_medallion/main.tf).

```text
CountRunningExecutions          sfn:listExecutions on this state machine
        │
   AlreadyRunning?  ──yes──▶  SkippedOverlappingRun  (Succeed)
        │ no
   MergeLayers  (Parallel)
        ├── MergeSilver       athena:startQueryExecution.sync
        └── MergeQuarantine   athena:startQueryExecution.sync
        │
   MergeGold                  athena:startQueryExecution.sync
```

The overlap guard exists because concurrent Iceberg merges scan the same data
twice and then fail on the commit lock. The two Silver branches run in parallel
because they write different tables from the same Bronze window. Gold waits for
both because it rebuilds only the partitions that moved, and it reads
`silver_trades` to find them.

`.sync` means Step Functions polls Athena directly. No Lambda sits in the loop,
so this stage deploys zero lines of code.

Run one cycle by hand with `make microbatch-aws`. Leave
`microbatch_enabled = false` on a first deploy, start an execution manually, and
read the graph before arming the schedule.

### 4.4 Cost shape

Kinesis on-demand and Firehose per-GB charges scale with traffic. Everything
else scales with hours the stack is up. The estimate in the design spec's §10
assumes roughly 130 hours a month, not continuous operation. That assumption
carries real weight: an armed schedule left running over a long weekend is the
easiest way to be surprised by a bill. Read the actual number from the "Data
scanned" column in the Athena console rather than trusting the arithmetic.

`aws_budgets_budget` emails at 80% of `monthly_budget_usd`. It reports; it does
not stop anything.

## 5. Gaps between deployed and designed

**Stack A.** `make up` creates every solid box in §3.2 and the Binance and
Coinbase half of §2. It does not create Kraken, Alpaca, or news connectors, DLQ
writers, `md.book.*` or `md.bars.v1` producers, or a Gold layer. The `_dlq.*`
topics exist per the design's dead-letter contract, but no code publishes to
them; an unparseable frame raises an exception and the WebSocket session
reconnects.

**Stack B.** `make up-aws` creates every solid box in §4.2 and fills Bronze,
Silver, and Gold from the live stream. Stages N4 (backfill and reconciliation),
N5 (point-in-time boundary), and N6 (agent) are designed and unbuilt, as are
slices E2 (order-book liquidity) and E4 (narrative). Table maintenance
(`OPTIMIZE`, `VACUUM`) is designed and unbuilt.

**Both.** Equity coverage stays IEX-only on Alpaca's free tier, roughly 2% of
volume; crypto carries the streaming workload. Nothing in either stack has run
in a real account since the last wipe, so every design assumption stays open
until someone brings it up.

Priority order for closing these gaps:
[`DATA_LAYER_NEXT_STEPS.md`](DATA_LAYER_NEXT_STEPS.md) §4.
