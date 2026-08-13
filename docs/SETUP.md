# Setup, Installation, and Prerequisites

This is the operational companion to the design docs — it answers "what do I
install, what do I click, and what do I type into which file" rather than
"why is it built this way." For the why, see:

- [`docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`](superpowers/specs/2026-08-07-finance-data-ai-platform-design.md) — parent design (accounts, ingestion, topics, AI layer sketch)
- [`docs/superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md`](superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md) — Databricks lakehouse contracts (design only, not yet implemented)

## 0. What is actually running today

Be precise about this before installing anything, so you don't go looking
for infrastructure that doesn't exist yet:

| Stage | Status | Where |
|---|---|---|
| Stage 0 — foundation (models, codec, gap detection, queue policy) | **Implemented** | `ingest/core/` |
| Stage 1 — streaming ingestion (Binance/Coinbase connectors, Kafka producer, AWS infra, local dev stack) | **Implemented** | `ingest/`, `infra/`, `docker/`, `scripts/` |
| Stage 2a — Bronze + Silver pipeline (Kafka → Bronze → Silver AUTO CDC, quarantine) | **Implemented, not yet run live** | `lakehouse/`, `resources/`, `databricks.yml` — deployed to `fdai.market`; a live run is blocked on the Databricks NAT EIP allowlist ([`docs/RUNBOOK_STAGE_2A.md`](RUNBOOK_STAGE_2A.md) §6) |
| Stage 2b/3 — Gold bars, backfill, semantic layer, vector index | **Designed, not implemented** | spec only — see §12 of the data-layer design for the remaining five plans |
| Stage 4+ — LLM trading-decision agent, serving, dashboards | **Sketched in the parent spec only** | no design doc, no code |

Everything in this document about AWS is for something that exists and that
`make up` provisions today. Everything about Databricks is a mix of "needed
today" (publishing Kafka connection secrets) and "get it right now so Stage
2 isn't blocked later" (Unity Catalog, CLI version, workspace network info).

## 1. Local tooling prerequisites

| Tool | Version | Why | Install (macOS) |
|---|---|---|---|
| Python | >= 3.12 | `pyproject.toml` `requires-python` | `brew install python@3.12` (or `uv python install 3.12`) |
| [`uv`](https://docs.astral.sh/uv/) | any recent | dependency manager; `uv.lock` is committed | `brew install uv` |
| Docker runtime | any recent | local Kafka via `docker compose` | `brew install docker`; this machine uses **Colima**, not Docker Desktop — `brew install colima && colima start` |
| Terraform | >= 1.9.0 | pinned in every `terraform {}` block | `brew install terraform` |
| AWS CLI v2 | any recent | credential testing, quota checks | `brew install awscli` |
| Databricks CLI | **>= v0.292.0** | secret-scope publishing, and every Stage 2 tool | `brew install databricks/tap/databricks` |
| git | any recent | | usually preinstalled |

The Databricks CLI version floor is not a guess: while drafting the Stage
2/3 design, the CLI installed in this environment was v0.280.0 and **every
configured profile reported `Valid = NO`** against it. Confirm your own
install before relying on it:

```bash
databricks --version                # must be >= 0.292.0
databricks auth profiles            # every profile you intend to use must show Valid = YES
```

### Clone and verify

```bash
git clone https://github.com/hailampy123/realtime-finance-bot.git
cd realtime-finance-bot
uv sync                             # installs runtime + dev dependency groups
make check                          # lint + typecheck + unit tests — must be green with zero cloud creds
```

If `make check` fails here, stop and fix it before touching AWS or
Databricks — nothing past this point should require cloud access.

## 2. Manual steps — AWS sandbox account (Account A)

Account A is wiped every 7 days and cannot create IAM roles (SCP-restricted).
Every resource it needs is created by `make up` (Terraform) — there is
deliberately no console click-through for infrastructure. The only manual
parts are things Terraform cannot do because they are about *your access*,
not the *resources*:

1. **Get credentials into your shell.** However your organization issues
   sandbox credentials (access key/secret, session token, SSO profile) — put
   them where the AWS SDK credential chain finds them: environment variables
   (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) or
   `AWS_PROFILE` pointing at a configured profile (`aws sso login` first, if
   your org uses SSO). This has to be redone every ~7 days when the account
   is wiped and reissued — there is no way to automate around that. Nothing
   in this repo's Terraform or scripts hardcodes a profile — every
   `provider "aws" {}` block relies on the AWS SDK's default credential
   chain, so setting `AWS_PROFILE` (or the raw key/secret/session-token
   triple) before running `make up`/`make down` is sufficient.

   If your org provisions sandbox access via **AWS IAM Identity Center
   (SSO)** — add a profile to `~/.aws/config` reusing your existing
   `sso-session`, then log in:

   ```ini
   [profile fdai-sandbox]
   sso_session    = <your-sso-session-name>
   sso_account_id = <SANDBOX_ACCOUNT_ID>
   sso_role_name  = <PERMISSION_SET_NAME>
   region         = ap-southeast-1
   ```

   ```bash
   aws sso login --profile fdai-sandbox
   aws sts get-caller-identity --profile fdai-sandbox   # confirms the account
   export AWS_PROFILE=fdai-sandbox
   ```

   If instead your org hands you **raw or temporary keys** (a portal-issued
   access key/secret, or an STS triple with a session token), add them under
   a dedicated profile rather than `default` — `aws configure --profile
   fdai-sandbox`, or by hand in `~/.aws/credentials`:

   ```ini
   [fdai-sandbox]
   aws_access_key_id     = ...
   aws_secret_access_key = ...
   aws_session_token     = ...   # only if the sandbox issues temporary creds
   ```

   Either way, avoid exporting `AWS_PROFILE` globally in your shell rc file
   if you also use AWS for other, unrelated work — it silently redirects
   every other `aws`/`terraform` invocation in every shell to the sandbox
   profile. Export it per-session, or scope it to this directory with
   `direnv` (a gitignored `.envrc` containing `export
   AWS_PROFILE=fdai-sandbox`).
2. **Confirm the region you're using.** Everything defaults to `ap-southeast-1`
   (`infra/bootstrap/variables.tf`, `infra/envs/dev/terraform.tfvars.example`).
   If your sandbox is scoped to a different region, set it consistently — see
   §5b/§5c.
3. **(Optional, first run only) Check the MSK service quota** in your region:
   AWS Console → Service Quotas → Amazon Managed Streaming for Apache Kafka.
   Fresh sandbox accounts occasionally start with a tightened default quota
   for broker count/type; if `make up` fails at the `kafka_msk` apply step
   with a quota error, this is where to request an increase.

That's the complete list. No IAM console, no VPC console, no EC2 console —
if `make up` can't create it, per the project's own rule, it doesn't exist.

## 3. Manual steps — Databricks workspace (Account B, permanent)

Account B is permanent and holds the Unity Catalog system of record. You
have workspace-admin there. Do this section now even though Stage 2 isn't
implemented yet — `make up`'s bootstrap script already publishes secrets
here today, and the rest unblocks Stage 2 the moment it starts.

1. **Upgrade the CLI and authenticate**, per §1. Then log in:
   ```bash
   databricks auth login --host https://<your-workspace-host>.cloud.databricks.com
   ```
   or configure a profile with a PAT (next step) via `databricks configure`.
   `scripts/bootstrap.sh` calls `databricks secrets create-scope` /
   `put-secret` using your default CLI auth context — make sure that context
   is the authenticated one, or export `DATABRICKS_CONFIG_PROFILE`.

2. **Generate a Personal Access Token — manual UI step, no CLI/API shortcut
   for the first one:**
   - Open your workspace in the browser.
   - Click your username (top right) → **Settings** → **Developer** →
     **Access tokens** → **Generate new token**.
   - Copy it immediately; it is shown once. Use it with `databricks configure`
     or `databricks auth login` if you're not using OAuth device login.

3. **Confirm you can create secret scopes — do this now, not later:**
   ```bash
   databricks secrets create-scope fdai-preflight
   databricks secrets delete-scope fdai-preflight
   ```
   Workspace-admin should already grant this. If it fails, check **Admin
   Settings → Identity and access**.

   **Why this is a separate pre-flight step.** `scripts/bootstrap.sh` runs
   `create-scope ... 2>/dev/null || true` — the `|| true` exists so re-runs
   survive "scope already exists", but it swallows a *permission* denial just
   as silently. You then fail on the next line with `RESOURCE_DOES_NOT_EXIST`
   about a scope that was never created, which points at the wrong problem.
   Worse, it happens at step **5 of 6**, after ~20 minutes of billed MSK
   provisioning. And since broker DNS changes on every rebuild, the secret
   scope is the only channel by which Databricks ever learns the endpoint —
   without it Stage 2 cannot run at all.

4. **Find the workspace's egress IP(s).** Which IPs depends on the compute
   type, and **classic and serverless egress from completely different
   places**. Get this wrong and Kafka reads fail with a connection timeout
   that looks like a broker problem.

   **4a. Classic compute — the stable `/32` (this is the one you need).**
   The Kafka reader runs on classic compute (see the note below), so this is
   the IP that matters. Nodes have no public IPs; all egress SNATs to the
   VPC's NAT Gateway Elastic IP.

   **Step 1 — count the availability zones.** Do this first; it determines how
   much work the rest is. In the workspace: **Compute → Create compute →
   Advanced → Instances → Availability Zone**. The dropdown lists the AZs this
   workspace can launch into.

   This matters because a NAT Gateway is per-AZ. Databricks recommends
   deploying "a NAT Gateway (and subnet) in each availability zone" for
   high availability, so a multi-AZ workspace plausibly has **one EIP per
   AZ** — and Databricks does **not** document the managed-VPC topology
   anywhere, so it cannot be looked up, only observed.

   **Step 2 — read the egress IP from each AZ.** On a **classic** cluster
   (single-node, smallest node type, 10-minute auto-terminate is plenty),
   pinned to a specific AZ via the dropdown above:

   ```python
   %sh curl -s https://checkip.amazonaws.com
   ```

   Repeat once per AZ. Collect the **distinct** IPs.

   - Single AZ → one IP, and you're done.
   - Multiple AZs all returning the **same** IP → single shared NAT Gateway.
     That's a useful positive result, not a wasted run — it proves one `/32`
     is sufficient.
   - Multiple AZs returning **different** IPs → allowlist every one.

   Getting this wrong is unpleasant to debug: allowlist only the AZ you
   happened to sample and the pipeline succeeds when the cluster lands there
   and times out when it doesn't, which reads as an intermittent broker fault
   rather than a firewall rule.

   Every distinct IP goes into `kafka_client_cidrs` as a `/32` (§5c),
   alongside your own IP.

   **If you do have AWS CLI access to Account B**, you can enumerate them
   directly instead — but note this is often unavailable for a managed or
   training workspace, and the EIP appears nowhere in the Databricks console
   or API (the account-level network object has no NAT or EIP field). Get the
   VPC ID from **Account Console → Workspaces → your workspace → network
   configuration**, then:

   ```bash
   aws ec2 describe-nat-gateways --region <workspace-region> \
     --filter Name=vpc-id,Values=<workspace-vpc-id> \
     --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text
   ```

   `--region` is required and must be the **workspace's** region — this
   command is region-scoped and silently returns nothing (no error) when
   pointed at the wrong one.

   In a Databricks-managed VPC these EIPs are Databricks-allocated, so you
   don't control their lifecycle; re-verify after any workspace network change.

   **Region note.** The workspace and the sandbox need not share a region — a
   security-group allowlist matches on IP, not region. A verified example: an
   egress IP of `3.105.165.32` falls in `3.104.0.0/14` = **ap-southeast-2**
   (Sydney), while `scripts/bootstrap.sh` defaults `REGION` to
   **ap-southeast-1** (Singapore). That split is benign here: at a 5-minute
   trigger cadence the extra ~90 ms RTT is noise, and because MSK is reached
   over a *public* endpoint the traffic is billed as internet egress either
   way, so co-locating would not save it. Confirm your own region with
   `curl -s https://ip-ranges.amazonaws.com/ip-ranges.json` rather than
   assuming.

   **4b. Serverless — only if something serverless must reach Kafka.**
   Nothing in the current design does, so you can normally skip this. If that
   changes, note that NCC "stable IPs" were **decommissioned on 2026-05-25**;
   the supported source is now
   [`ip-ranges.json`](https://www.databricks.com/networking/v1/ip-ranges.json).
   Filter `service=Databricks`, `type=outbound`, `platform=aws`, and **your
   workspace's** region — which is the region you established in 4a, not
   necessarily the sandbox's. As published 2026-07-29:

   | workspace region | outbound prefixes |
   |---|---|
   | `ap-southeast-2` (Sydney) | `13.237.96.217/32`, `3.26.4.0/28`, `3.27.139.0/24` |
   | `ap-southeast-1` (Singapore) | `13.213.212.4/32`, `13.214.1.96/28`, `47.128.12.0/24` |

   Note these are **CIDR blocks, not `/32`s** — `kafka_client_cidrs` takes
   CIDRs, so no variable change is needed, but a `/32`-shaped assumption will
   drop most of the range.

   These rotate (published as often as every 30 days), so a one-time copy
   eventually breaks. Allowlisting them also opens the broker at L3 to a
   Databricks-wide shared range, so SASL/SCRAM — not the IP allowlist — is
   the real access control.

   **Keep the Kafka reader on classic compute.** AUTO CDC needs *"serverless
   Lakeflow pipelines **or** the Lakeflow pipelines `Pro` or `Advanced`
   editions"*, so classic + Pro/Advanced satisfies the data-layer design
   while preserving a stable `/32` you control. Serverless stays confined to
   metric-view materialization and Vector Search, neither of which touches
   Kafka.

   **Also check the network policy** — it can block external egress
   regardless of your MSK security group:

   ```bash
   databricks account workspace-network-configuration \
     get-workspace-network-option-rpc --workspace-id <ID>
   ```

   Under Restricted Access, serverless reaches only UC external locations and
   policy-listed FQDNs; an external Kafka broker is blocked outright.

5. **Note down, for Stage 2 planning** (per the data-layer spec's own
   "Assumptions to verify" list — don't skip this, several Stage 2 designs
   have no fallback if these are wrong):
   - Is Unity Catalog enabled? (Admin Console → Catalog)
   - Is Serverless compute enabled? (needed for `*_current` materialized
     metric views and Vector Search)
   - DBR version available for new clusters (Metric Views with semantic
     metadata need DBR 17.3+; 16.4+ still supports creation without it)

## 4. GitHub repository

Already created and set to **public**
(`https://github.com/hailampy123/realtime-finance-bot`) — deliberately, so
the EC2 producer host's boot script (`infra/modules/producer_host/user_data.sh.tftpl`)
can `git clone` it without embedding credentials in an ephemeral, wiped
account.

```bash
git clone https://github.com/hailampy123/realtime-finance-bot.git
```

## 5. Configuration variables

### 5a. Local application config (`INGEST_*`, read by `ingest/settings.py`)

| Variable | Default | Required for local `make compose-up`? | Purpose |
|---|---|---|---|
| `INGEST_BOOTSTRAP_SERVERS` | `localhost:9092` | no | Kafka bootstrap servers |
| `INGEST_SASL_USERNAME` | `None` | no (only against MSK) | SCRAM username |
| `INGEST_SASL_PASSWORD` | `None` | no (only against MSK) | SCRAM password |
| `INGEST_UNIVERSE_PATH` | `config/universe.yaml` | no | instrument universe file |
| `INGEST_VENUES` | `["binance", "coinbase"]` | no | which connectors the runner starts |
| `INGEST_QUEUE_MAXSIZE` | `20000` | no | bounded per-topic queue size |
| `INGEST_TRADES_TOPIC` | `md.trades.v1` | no | output topic name |

No `.env` file is committed. Create one at the repo root (or export the
vars) only if you want to override a default for a local, non-Docker run —
`Settings` reads `env_file=".env"` automatically. Inside the cloud producer
host, these are written into `/opt/fdai/app/.env` directly by
`user_data.sh.tftpl` — you never hand-edit that one either.

### 5b. Terraform — `infra/bootstrap` (state backend, Account A)

| Variable | Default | Must you set it? |
|---|---|---|
| `project` | `"fdai"` | no |
| `region` | `"ap-southeast-1"` | only if your sandbox uses a different region |

No `.tfvars` file needed — `scripts/bootstrap.sh` passes both via `-var=`
from its `PROJECT`/`AWS_REGION` shell env vars.

### 5c. Terraform — `infra/envs/dev` (the actual sandbox stack, Account A)

Create this file — it is gitignored and does not exist yet:

```bash
cp infra/envs/dev/terraform.tfvars.example infra/envs/dev/terraform.tfvars
```

| Variable | Default | Must you set it? | Notes |
|---|---|---|---|
| `kafka_client_cidrs` | **none** | **yes, but `[]` is valid** | `list(string)`. **Only the Databricks NAT EIP from §3.4**, as a `/32`. Do *not* add your own IP: `make up` detects it and appends it every run, so a hand-written one silently goes stale the next time you change networks. Empty until you have looked the EIP up — everything except Databricks-to-Kafka works without it. |
| `repo_url` | **none** | **yes, required** | Set to `https://github.com/hailampy123/realtime-finance-bot.git`. A placeholder here does not fail the apply: the instance is created, then `user_data` dies on `git clone` under `set -e`, so you get a running host with no producer and no `.env`. |
| `region` | `"ap-southeast-1"` | no | Should match `AWS_REGION` (§5d), which the *state backend* uses. They are read independently and can drift; `make up` warns when they do. |
| `project` | `"fdai"` | no | |
| `repo_ref` | `"main"` | no | branch/ref the producer host clones |
| `msk_public_access` | `false` | **no — do not hand-set** | Sequenced by `make up`. AWS rejects public access on a still-`CREATING` cluster, and again unless `msk_restrict_acls` is already true. |
| `msk_restrict_acls` | `false` | **no — do not hand-set** | Sets `allow.everyone.if.no.acl.found=false`, MSK's precondition for public access. Turning it on before the ACLs exist locks every client out — see §5f. `make unlock` is the way back. |
| `operator_cidrs` | `[]` | **no — detected** | Your current `/32`, passed by `scripts/bootstrap.sh`. Grants producer-host SSH (for the ACL bootstrap) and is appended to `kafka_client_cidrs`. |
| `instance_profile_name` | `null` | no — leave `null` | the sandbox cannot create instance profiles; this only matters if the account ever provides a pre-existing one. |

Never hand-edit or commit `backend.tf` (rendered from `backend.tf.tftpl` by
`scripts/bootstrap.sh`'s `sed` step) or `.terraform.lock.hcl` — both are
gitignored and regenerated on every `make up`.

### 5d. Shell environment variables (read by scripts, not by Terraform directly)

| Variable | Default | Purpose |
|---|---|---|
| `PROJECT` | `fdai` (Makefile) | resource-name prefix; flows into both Terraform layers and the Databricks secret scope name |
| `AWS_REGION` | `ap-southeast-1` (bootstrap.sh) | region for both Terraform layers |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` or `AWS_PROFILE` | — | standard AWS SDK credential chain; required for every `terraform apply`/`destroy` |
| `FDAI_TARGET` | `local` | which broker the dev notebooks read from: `local`, `msk` (uses `INGEST_*` above), or `terraform` (reads the live endpoint from the stack outputs). See [`notebooks/README.md`](../notebooks/README.md). |

### 5e. Databricks-side state (written by `make up`, never hand-configured)

A secret scope named `${PROJECT}` (default `fdai`) holding `kafka_bootstrap`,
`kafka_username`, `kafka_password` — republished on every `make up` because
the broker DNS changes on every rebuild, so nothing downstream should ever
hardcode it.

### 5f. Why MSK public access needs Kafka ACLs, and why the order is fixed

This is the single least obvious part of `make up`, and getting it wrong
costs a full cluster rebuild — so it is worth reading once.

MSK refuses `UpdateConnectivity` unless the broker configuration sets
`allow.everyone.if.no.acl.found=false`:

```text
BadRequestException: The allow.everyone.if.no.acl.found configuration setting
must be set to false when public access is turned on.
```

That setting is not a formality. It switches Kafka's authorizer to
deny-by-default, so **every** SCRAM principal is refused unless an ACL allows
it — including a principal trying to *create* ACLs, since `CreateAcls` needs
`Alter` on the cluster. A cluster tightened before its ACLs exist cannot be
repaired from any Kafka client. AWS states the order plainly: *"you must first
set Apache Kafka ACLs for your cluster. Then, update the cluster's
configuration."*

Which forces an awkward shape. ACLs can only be written over a reachable
broker; before public access the in-VPC endpoint is the only one that
resolves; and the producer host is the only thing inside the VPC. So the ACL
bootstrap runs *there*, over SSH, on a keypair Terraform generates into
`infra/envs/dev/.ssh/` (gitignored, regenerated every `make up`).

The grant is `User:*` — any authenticated principal, full access. That matches
the security model this project already states: SASL/SCRAM plus the
security-group allowlist is the access control, not per-topic authorization.
Narrowing it would mean an ACL per new topic and consumer group, where a
missing one shows up as a confusing runtime auth error.

Two further constraints, both discovered the hard way:

- **Steps 2 and 3 cannot be one apply.** The AWS provider updates
  connectivity *before* configuration within a single cluster update, so a
  combined apply attempts public access first and fails with the same error as
  doing nothing.
- **The wait is on a readiness file, not a timer.** `user_data` touches
  `/opt/fdai/ready` as its last line; `bootstrap.sh` polls for it over SSH.
  Sleeping instead would risk tightening the cluster before the ACLs land.

If a bootstrap does die between the ACL grant and public access, the cluster
may be left denying everything. Recover with:

```bash
make unlock    # allow.everyone.if.no.acl.found=false -> true, public access off
make up        # re-runs the sequence in order
```

## 6. Data sources — context and what's actually used

### 6a. Implemented today

**Binance — `wss://stream.binance.com:9443/stream?streams=<symbol>@aggTrade`**

- Subscribes to **`@aggTrade`**, not `@trade`, specifically because
  `data.binance.vision`'s public archive publishes an `aggTrades/` series
  whose id space matches the live stream's `a` field. That shared id space
  is what makes the keyless REST endpoint `/api/v3/aggTrades?fromId=` usable
  for exact-range gap repair with **no API key**. `@trade` would require
  `/api/v3/historicalTrades`, which does need a key, and would also put live
  and future-archived data in different id spaces (a correctness trap called
  out explicitly in the data-layer spec).
- Fields consumed: `a` (aggregate trade id → sequence number and repair
  cursor), `p` (price, kept as string), `q` (size, kept as string), `T`
  (trade time in ms, converted to µs), `m` (`isBuyerMaker` — **inverted**:
  `m=true` means the buyer was the maker, so the *seller* crossed the spread
  → `side=SELL`; `m=false` → `side=BUY`), `s` (symbol).
- Rate limit: REST is used only for gap repair, via a token bucket
  (`ingest/core/ratelimit.py`) of 10 req/s, burst 20 — deliberately below
  Binance's published weight-based ceiling (~6000/min spot).
  Sequence tracking does **not** reset on reconnect
  (`resets_sequence_on_reconnect = False`), because the aggregate trade id is
  persistent across the exchange — resetting would mask a real gap that
  happened during the disconnect. Binance also drops WS connections at 24h;
  the connector reconnects proactively at ~23h.

**Coinbase — Advanced Trade `market_trades` channel,
`wss://advanced-trade-ws.coinbase.com`**

- Fields consumed: `trade_id`, `price`, `size`, `side` (falls back to
  `UNKNOWN` if the field is missing rather than raising — a real parsing bug
  fixed during review), `time` (RFC3339, converted to µs), `sequence_num`.
- `sequence_num` is **connection-wide**, not per product — gaps are tracked
  under a wildcard symbol (`"*"`). Coinbase's public market-data API has no
  id-range query, so repair is **best-effort**: refetch the most recent
  trades for every configured product and rely on natural-key (`trade_id`)
  dedupe downstream to absorb any overlap. Sequence tracking *does* reset on
  reconnect here, unlike Binance, because there is no persistent
  cross-reconnect id space to protect.
- Rate limit: token bucket at 8 req/s, burst 10 (public tier).

**Symbol universe (`config/universe.yaml`)** — 8 crypto pairs, each mapped to
both venues' native symbol spelling: BTC-USD (`BTCUSDT`/`BTC-USD`), ETH-USD,
SOL-USD, XRP-USD, ADA-USD, LINK-USD, AVAX-USD, DOGE-USD. `InstrumentMap`
raises on an unmapped symbol rather than guessing.

### 6b. Designed, not yet implemented — needs API keys when built

These appear in the parent spec's architecture diagram and topic table, but
`ingest/connectors/` currently has only `binance.py` and `coinbase.py`:

| Source | Feeds | What it needs | Status |
|---|---|---|---|
| Kraken WS | future `md.trades.v1` rows | no key (public WS) | rate-limit bucket already reserved (`1 req/s`) in `ratelimit.py`, but no connector code |
| Alpaca IEX WS | equities (~2% of US volume — free-tier limitation, documented in the README, not a bug) | Alpaca API key + secret | not started |
| Alpaca news WS / Finnhub REST poll | `news.articles.v1` | Alpaca and/or Finnhub API key | not started; no `news.v1.avsc` schema exists yet either — a named precondition in the data-layer spec before news can be indexed for the agent's vector search |
| `data.binance.vision` public archives (`aggTrades/`, `klines/1m`, `klines/1s`) | 2-year historical backfill (deep/mid/hot tiers) | none — public HTTPS ZIPs | design-only (data-layer spec §6, stage 3a); loader needs checksum verification, per-file timestamp-unit detection (ms vs µs changes partway through Binance's archive history), and the same `isBuyerMaker` inversion as the live connector |

None of these block `make up` today. They're listed here so that whichever
one you pick up next, you know upfront what account/signup it needs before
writing the connector.

## 7. Step-by-step walkthrough

### 7a. Local-only loop (no cloud credentials needed)

```bash
git clone https://github.com/hailampy123/realtime-finance-bot.git
cd realtime-finance-bot
uv sync
make check              # lint + typecheck + unit tests
make compose-up         # single-broker Kafka on localhost:9092
make test-integration   # end-to-end against the local broker
make compose-down
```

To actually watch data move without any cloud credentials, in two terminals:

```bash
make stream-local       # compose Kafka + topics + host-run Binance/Coinbase producers
make notebook TARGET=local  # local broker; installs the `notebook` group
make notebook TARGET=msk    # refreshes MSK access, verifies it, then starts Jupyter
```

`make compose-up` on its own leaves you with an **empty** broker — auto-create
is off, so there are no topics until something creates them. `make stream-local`
does that and then runs the producers on the host, which is the only local path
that delivers messages (§8, first bullet).

### 7b. Full cloud bootstrap

1. Complete §2 (AWS credentials in your shell) and §3 (Databricks CLI
   upgraded + authenticated, NAT EIP located).
2. `cp infra/envs/dev/terraform.tfvars.example infra/envs/dev/terraform.tfvars`
   and set `repo_url` per §5c. `kafka_client_cidrs` takes only the Databricks
   NAT EIP, and `[]` is fine if you don't have it yet — your own IP is
   detected on every run.
3. `make up` — runs `scripts/bootstrap.sh`, eight steps:

   | # | Step | Notes |
   |---|---|---|
   | 1–2 | state backend, render `backend.tf` | |
   | 3 | apply the stack — MSK private, ACLs permissive | ~25–30 min on first create; near-instant on a re-run (see below) |
   | 4 | wait for the producer host | polls `/opt/fdai/ready` over SSH, not a timer |
   | 5 | grant Kafka ACLs from inside the VPC | `scripts/create_acls.py` on the producer host, idempotent |
   | 6 | enforce ACLs (`allow.everyone.if.no.acl.found=false`) | MSK's precondition for public access |
   | 7 | enable MSK public access | must be its own apply — see §5f |
   | 8 | topics, Databricks secrets, smoke test | |

   Steps 3, 6, and 7 are three separate cluster operations, each of which AWS
   applies serially, so **budget 45–60 minutes** on the first-ever run rather
   than the 20 this originally targeted. Why the order cannot be compressed
   is §5f.

   For what each AWS resource in this stack actually *is* and why it's there —
   written for someone new to Terraform and AWS — see
   [`docs/MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md).

   Step 3 also checks whether the cluster already finished a previous run
   (already public, already locked down) before deciding what to apply. If
   so, it asserts that end state directly instead of walking back through
   "permissive" — doing the latter would try to loosen ACL enforcement while
   public access is still on, which AWS refuses for the same reason it
   refuses turning public access on with no ACLs. Steps 6–7 are then skipped
   as already done. This makes `make up` safe to re-run after a failure in a
   later step without touching the cluster again.
4. Read the final `Ready.` block for the public/private broker DNS, the
   producer host's IP, and the `ssh` command for it.
5. `make down` when finished — destroys the sandbox stack and the state
   backend. Databricks is untouched.
6. `make rebuild` (= `down` + `up`) is the normal way to survive the
   account's 7-day wipe cycle.
7. `make unlock` if a run dies between steps 5 and 7 and leaves the cluster
   denying every client — see §5f.

## 8. Known gaps and follow-ups

Not blockers for `make up`, but worth knowing before you rely on them:

- **`docker compose --profile live` cannot deliver messages to the local
  broker** — a single-listener Kafka-in-Docker limitation; use `make up`
  against real MSK, or the integration tests (which connect from the host),
  instead. See the README's "Known limitations."
- **DLQ topics are provisioned but nothing writes to them yet** — an
  unparseable frame currently causes a WebSocket reconnect rather than a
  dead-letter publish.
- **Databricks assumptions listed in §3.5 are unverified** until you check
  them — the data-layer spec names two (Unity Catalog enabled, AUTO CDC
  available) that would force a structural redesign if false; the rest
  degrade gracefully with a named fallback (spec §11).
- **The producer host is no longer strictly egress-only.** `make up` opens
  port 22 to the operator's current `/32` because the Kafka ACL bootstrap can
  only run from inside the VPC (§5f). The keypair is generated per run into
  `infra/envs/dev/.ssh/` and gitignored. A plain `terraform apply` without
  `operator_cidrs` creates no SSH rule at all, so the exposure lasts only as
  long as the stack.
- **`make up` now takes 45–60 minutes, not 20.** Three serial MSK cluster
  operations (create, configuration, connectivity) rather than two. The
  README's original target predates the ACL requirement.
- **Kafka authorization is deliberately wide open to authenticated users.**
  The ACL grant is `User:*` with full access, so SASL/SCRAM and the
  security-group allowlist are the only access control. If this ever carries
  data that needs per-topic isolation, `scripts/create_acls.py --principal`
  narrows the grant, and every topic and consumer group then needs its own
  ACL provisioned.
