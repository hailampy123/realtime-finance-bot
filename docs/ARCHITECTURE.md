# Architecture — Current State

Two diagrams, both drawn from what actually exists in this repo today (source
files cited under each), not from the aspirational end-state in the parent
design spec. Solid green = built and live after `make up`. Dashed gray =
designed or sketched, no code yet. If a box isn't green, `make up` does not
create it.

For the target end-state these diagrams intentionally do **not** show, see
[`docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`](superpowers/specs/2026-08-07-finance-data-ai-platform-design.md)
(parent architecture) and
[`docs/superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md`](superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md)
(lakehouse). For install/config steps, see [`docs/SETUP.md`](SETUP.md). For a
resource-by-resource explanation of what `make up` creates and why — written for
someone new to Terraform and AWS — see
[`docs/MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md). Two more written for the same
audience: [`docs/AWS_SERVICES_EXPLAINED.md`](AWS_SERVICES_EXPLAINED.md) (what each
AWS service is, with inspection commands) and
[`docs/KAFKA_EXPLAINED.md`](KAFKA_EXPLAINED.md) (Kafka, grounded in this repo's
own config), and [`docs/CODEBASE_EXPLAINED.md`](CODEBASE_EXPLAINED.md)
(directory-by-directory tour, module boundaries, where to make a change).

## 1. Data flow — what's actually moving today

```mermaid
flowchart LR
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5

    BinanceWS["Binance WS\nwss://.../@aggTrade"]:::built
    CoinbaseWS["Coinbase WS\nmarket_trades channel"]:::built
    KrakenWS["Kraken WS"]:::planned
    AlpacaWS["Alpaca IEX WS"]:::planned
    NewsWS["Alpaca news WS /\nFinnhub REST poll"]:::planned
    Archive["data.binance.vision\narchives (backfill)"]:::planned

    BinanceConn["BinanceConnector\ningest/connectors/binance.py"]:::built
    CoinbaseConn["CoinbaseConnector\ningest/connectors/coinbase.py"]:::built

    BinanceWS --> BinanceConn
    CoinbaseWS --> CoinbaseConn
    KrakenWS -.-> NoConn["no connector code yet"]:::planned
    AlpacaWS -.-> NoConn
    NewsWS -.-> NoConn

    Queue["BoundedTopicQueue\nper-topic backpressure policy\ningest/core/queue.py"]:::built
    Runner["IngestRunner\ngap detection + REST repair\ningest/runner.py"]:::built

    BinanceConn --> Queue
    CoinbaseConn --> Queue
    Queue --> Runner
    Runner -. REST gap repair .-> BinanceConn
    Runner -. REST gap repair .-> CoinbaseConn

    Producer["TradeProducer\nbare Avro + SASL/SCRAM\ningest/core/producer.py"]:::built
    Runner --> Producer

    subgraph MSK["MSK topics (scripts/create_topics.py)"]
        Trades["md.trades.v1\n← populated"]:::built
        Others["md.book.top.v1, md.book.depth.v1,\nmd.bars.v1, news.articles.v1,\nops.metrics.v1, _dlq.*\n← provisioned, empty"]:::planned
    end

    Archive -.-> BronzeArchive["bronze_trades_archive\n(design only)"]:::planned
    Producer --> Trades
    Producer -.-> Others

    ConsumeExample["scripts/consume_example.py\nmanual, ad-hoc VWAP demo"]:::built
    Trades --> ConsumeExample

    DBXStream["Databricks Structured Streaming\nBronze → Silver → Gold\n(design only, no code)"]:::planned
    Trades -.-> DBXStream
    BronzeArchive -.-> DBXStream
```

**Reading this diagram:** the only two connectors that exist are Binance and
Coinbase; both write to `md.trades.v1` and nothing else, because no other
topic's producer code exists yet. Every trade sits in Kafka until either
`scripts/consume_example.py` is run manually or Stage 2 (Databricks
Structured Streaming) is implemented — right now, neither runs continuously.

## 2. Deployed AWS infrastructure — physical topology

```mermaid
flowchart TB
    classDef built fill:#d4f7dc,stroke:#2f9e44,color:#1b4332,stroke-width:2px
    classDef planned fill:#f1f3f5,stroke:#adb5bd,color:#495057,stroke-width:1px,stroke-dasharray: 5 5
    classDef acct fill:#eef2ff,stroke:#4c51bf,color:#2a2f6b,stroke-width:2px

    GitHub["GitHub\nhailampy123/realtime-finance-bot\n(public repo)"]:::built

    subgraph AcctA["ACCOUNT A — sandbox, wiped every 7 days"]
        direction TB

        subgraph Bootstrap["State backend (infra/bootstrap) — local Terraform state"]
            S3["S3 bucket\nfdai-tfstate-<account-id>\nversioned, AES256\npublic access blocked"]:::built
            DDB["DynamoDB\nfdai-tflock"]:::built
        end

        subgraph VPC["VPC 10.42.0.0/16 (infra/modules/network)"]
            IGW["Internet Gateway"]:::built
            SubnetA["Public subnet AZ-0"]:::built
            SubnetB["Public subnet AZ-1"]:::built
            SGProducer["SG: producer\negress + operator SSH /32"]:::built
            SGMsk["SG: msk\n9196 from kafka_client_cidrs\n9096 from VPC only"]:::built

            EC2["EC2 t3.small (AL2023)\nno instance profile\nDocker image built locally\ninfra/modules/producer_host"]:::built
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

        Secret["Secrets Manager\nAmazonMSK_fdai_producer\n(SCRAM username+password)"]:::built
        KMS["Customer-managed KMS key\nalias/fdai-msk-scram"]:::built
        Policy["Resource policy:\nPrincipal = kafka.amazonaws.com\n(not an IAM role)"]:::built

        KMS --> Secret
        Policy -. grants read .-> Secret
        Secret -->|aws_msk_scram_secret_association| MSKCluster
        GitHub -->|"git clone (unauthenticated,\nno instance profile needed)"| EC2
        EC2 -->|SASL/SCRAM produce| MSKCluster
    end

    subgraph AcctB["ACCOUNT B — Databricks workspace, permanent\n(you hold workspace admin)"]
        direction TB
        NAT["NAT Gateway\nEIP allowlisted in SGMsk"]:::built
        SecretScope["Databricks secret scope 'fdai'\nkafka_bootstrap / kafka_username /\nkafka_password\n(republished every make up)"]:::built
        UC["Unity Catalog\nBronze/Silver/Gold\n(design only, no pipeline yet)"]:::planned
    end

    Operator["Your machine\nscripts/bootstrap.sh"]:::built
    Operator -->|"SSH 22, operator /32 only:\nKafka ACL bootstrap\n(scripts/create_acls.py)"| EC2

    MSKCluster -->|"public bootstrap endpoint\nSASL_SSL:9196"| NAT
    NAT -.-> UC
    Secret -.->|"databricks CLI, run\nfrom your machine\n(scripts/bootstrap.sh)"| SecretScope

    class AcctA,AcctB acct
```

**Reading this diagram:** every solid box in Account A is created and
destroyed by `make up` / `make down` (Terraform). The only thing that
persists Account A's state between runs is the S3 bucket in the same
account — which is why losing it on wipe is safe (§ the README's
reproducibility contract). The single link into Account B is the MSK public
bootstrap endpoint, reachable only from the CIDRs in `kafka_client_cidrs`
(the Databricks NAT EIP and your own IP) — no IAM role, no VPC peering, no
Private Link.

The one edge that looks out of place is the SSH arrow. MSK will not enable
public access unless `allow.everyone.if.no.acl.found` is false, which makes
Kafka deny-by-default, which means ACLs must already exist — and ACLs can only
be written over a reachable broker, which before public access means from
inside the VPC. The producer host is the only thing in there. Hence a
per-run keypair and a `/32` SSH rule that exist for exactly one step of
`make up`. The full reasoning is in [`docs/SETUP.md`](SETUP.md) §5f.

## 3. What "final deployed state (for now)" means

Running `make up` today gets you the entirety of diagram 2, plus the
Binance/Coinbase half of diagram 1. It does **not** get you: Kraken/Alpaca/
news connectors, DLQ writers, `md.book.*`/`md.bars.v1` producers, or anything
in Account B beyond the three secret values. Those are the concrete gaps
between "deployed" and "designed" as of this writing.
