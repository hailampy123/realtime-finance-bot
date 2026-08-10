# Local dev notebooks

An interactive loop over the live trade streams, separate from the runtime
package. Five notebooks over one small helper library ([`devlab/`](../devlab)):

| Notebook | Question it answers |
|---|---|
| [`00_stream_health.ipynb`](00_stream_health.ipynb) | Is the stream alive, and is it complete? Topic contents, partition balance, live arrival rate, sequence gaps, consumer lag. |
| [`01_explore_trades.ipynb`](01_explore_trades.ipynb) | What is actually in the data? A bounded window as a DataFrame, price and volume plots, ingest latency, Binance vs. Coinbase spread. |
| [`02_prototype_silver.ipynb`](02_prototype_silver.ipynb) | Are the Stage 2 transforms right? Natural-key dedupe and event-time bars in pandas, each with the PySpark it maps to. |
| [`03_msk_live_experiment.ipynb`](03_msk_live_experiment.ipynb) | A quick, minimal look at the deployed MSK cluster specifically — unlike the others, it calls `devlab.from_terraform()` directly rather than the switchable `devlab.resolve()`, so there's no ambiguity about which broker it's reading. |
| [`04_stream_product_research.ipynb`](04_stream_product_research.ipynb) | What does one selected stream reveal about source quality, latency, activity, volatility, cross-venue structure, and the next Bronze/Silver/Gold data products? |

## Setup

Notebook dependencies live in an opt-in `notebook` group, so `uv sync`,
`make check`, and the producer image never install jupyter or pandas:

```bash
make notebook TARGET=local
make notebook TARGET=msk
```

The MSK form is the supported entry point. It selects the AWS profile, refreshes
an expired SSO session when possible, updates the laptop's current `/32` through
Terraform, verifies Kafka metadata access, and passes the same environment to
Jupyter. Override the default profile only when necessary:

```bash
make notebook TARGET=msk FDAI_AWS_PROFILE=another-profile
```

## Getting data to look at

The notebooks read; something else has to write. Pick a target:

### Local (no cloud credentials, no cost)

```bash
make stream-local      # leave running in its own terminal
```

That brings up the compose broker, creates the topics, and runs the
Binance + Coinbase producers **on the host**. The host part is not incidental:
the `producers` compose service cannot reach the broker from inside a sibling
container, because Kafka advertises the host loopback address and there is
only one listener (see the README's "Known limitations"). Running the producer
on the host sidesteps it. `make compose-up` alone gives you an empty broker
with auto-create disabled — no topics, no data.

### MSK

```bash
AWS_PROFILE=fdai-sandbox make up  # first deployment only, ~20 min
make notebook TARGET=msk         # every notebook run after that
```

The launch preflight reads the live endpoint and SCRAM credentials from the
stack. No `.env` credentials, endpoint copy, profile export, or IP command is
required. `devlab.from_terraform()` also falls back to the AWS API if Terraform
temporarily cached an empty public endpoint:

```python
target = devlab.from_terraform()
```

## Switching targets

Notebooks `00`–`02` and `04` inherit the target chosen by the launch command.
`make notebook TARGET=local` passes `FDAI_TARGET=local`; the MSK form passes
`FDAI_TARGET=terraform` so the live stack outputs are used. Notebook `03` is
deliberately pinned to MSK. The research notebook still analyzes one target per
run and keeps only the run-depth choice in the notebook:

```python
RUN_MODE = "quick"  # "quick" (20k/60s) or "deep" (200k/10min)
```

`04_stream_product_research.ipynb` is strictly read-only. It uses
`devlab.local()` for local mode and live Terraform outputs through
`devlab.from_terraform()` for MSK mode; it never writes Kafka offsets, topics,
files, or infrastructure.

## The library

[`devlab/`](../devlab) is deliberately small, and lint/typechecked alongside
`ingest`:

- `config` — target resolution and credentials. Passwords are kept out of
  `repr`, since a bare `target` in a cell echoes its value.
- `stream` — `tail()` and `collect()`. **Every read is bounded by both a
  message count and a wall clock**, and at least one must be set. A cell
  polling a quiet topic forever is indistinguishable from a broken broker, so
  it is made impossible rather than documented against. Missing topics and
  unreachable brokers raise immediately with the command to run next.
- `health` — topic/partition watermarks, live rate, consumer lag, and sequence
  gaps. Returns dataclasses, not DataFrames, so it works without pandas.
- `frames` — pandas conversion plus `dedupe()` and `bars()`. Needs the
  `notebook` group.

Decoding goes through the same `trade_codec()` the producer encodes with, so
there is one schema in play here too.

## Committing notebooks

Outputs are stripped before commit — they hold real market data and make
diffs unreadable:

```bash
make notebook-clean    # nbstripout notebooks/*.ipynb
```

`make check` lints notebook code cells too (ruff reads `.ipynb` natively).

## Troubleshooting

**`Connect to ipv6#[::1]:9092 failed: Connection refused`** — the local
configuration is stale. The repository now uses `127.0.0.1:9092` explicitly;
recreate Kafka with `make compose-down && make stream-local` so the broker
advertises the corrected address.

**A read returns nothing.** Check in this order: is `make stream-local` still
running in its other terminal; does `devlab.topics(target)` show a non-zero
count for `md.trades.v1`; are you on `offset_reset="latest"` (which only sees
what arrives *after* the call) when you meant `"earliest"`.

**`TopicMissing`** — the broker is reachable but the topic is not there.
`make compose-up` alone does not create topics and auto-create is off;
`make stream-local` does.

**`TargetError: AWS credentials are invalid or expired`** — fully stop the
current Jupyter server and restart through `make notebook TARGET=msk`. The
preflight removes stale raw credential variables, uses the selected profile,
opens SSO login when it needs refreshing, and gives the new kernel the same
clean environment. For a non-SSO profile whose keys really expired, replace
the keys in that profile and run the same command again.

**`BrokerUnavailable` after changing networks** — fully stop Jupyter and run
`make notebook TARGET=msk` again. The preflight detects the new public IP and
updates both Terraform-managed operator rules before launching the server.

## Tests

`tests/devlab/` covers the pure logic. The pandas-dependent tests skip under a
plain `make test`; run the full set with:

```bash
make notebook-test
```
