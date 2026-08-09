# Local dev notebooks

An interactive loop over the live trade streams, separate from the runtime
package. Four notebooks over one small helper library ([`devlab/`](../devlab)):

| Notebook | Question it answers |
|---|---|
| [`00_stream_health.ipynb`](00_stream_health.ipynb) | Is the stream alive, and is it complete? Topic contents, partition balance, live arrival rate, sequence gaps, consumer lag. |
| [`01_explore_trades.ipynb`](01_explore_trades.ipynb) | What is actually in the data? A bounded window as a DataFrame, price and volume plots, ingest latency, Binance vs. Coinbase spread. |
| [`02_prototype_silver.ipynb`](02_prototype_silver.ipynb) | Are the Stage 2 transforms right? Natural-key dedupe and event-time bars in pandas, each with the PySpark it maps to. |
| [`03_msk_live_experiment.ipynb`](03_msk_live_experiment.ipynb) | A quick, minimal look at the deployed MSK cluster specifically — unlike the others, it calls `devlab.from_terraform()` directly rather than the switchable `devlab.resolve()`, so there's no ambiguity about which broker it's reading. |

## Setup

Notebook dependencies live in an opt-in `notebook` group, so `uv sync`,
`make check`, and the producer image never install jupyter or pandas:

```bash
make notebook          # uv sync --group notebook, then jupyter lab notebooks/
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
container, because Kafka advertises `PLAINTEXT://localhost:9092` and there is
only one listener (see the README's "Known limitations"). Running the producer
on the host sidesteps it. `make compose-up` alone gives you an empty broker
with auto-create disabled — no topics, no data.

### MSK

```bash
make up                # provisions the sandbox, ~20 min
export FDAI_TARGET=msk # then fill INGEST_SASL_* in .env
```

Or skip `.env` entirely and read the endpoint from the stack, which cannot go
stale — broker DNS is regenerated on every `make up`:

```python
target = devlab.from_terraform()
```

## Switching targets

Every notebook opens with `devlab.resolve()`, which reads `$FDAI_TARGET`
(`local` by default, or `msk`, or `terraform`). No notebook mentions a
hostname, so flipping the variable moves all three with no edits.

```python
import devlab

target = devlab.resolve()  # or devlab.resolve("msk")
```

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

**`Connect to ipv6#[::1]:9092 failed: Connection refused`** — noise, not an
error. librdkafka resolves `localhost` to both `::1` and `127.0.0.1` and tries
IPv6 first; the compose broker binds IPv4 only, so it falls back and connects.
It also appears during `make stream-local` and `scripts/create_topics`.
Silence it with `bootstrap="127.0.0.1:9092"` if it bothers you.

**A read returns nothing.** Check in this order: is `make stream-local` still
running in its other terminal; does `devlab.topics(target)` show a non-zero
count for `md.trades.v1`; are you on `offset_reset="latest"` (which only sees
what arrives *after* the call) when you meant `"earliest"`.

**`TopicMissing`** — the broker is reachable but the topic is not there.
`make compose-up` alone does not create topics and auto-create is off;
`make stream-local` does.

**`TargetError: AWS credentials are invalid or expired`** from
`devlab.from_terraform()` — the credentials in *this process* are stale, which
is not necessarily true in your terminal. Sandbox session tokens commonly
expire in hours, well before the account's 7-day wipe, and a running Jupyter
kernel never sees a terminal's credentials refresh after the kernel started.
Check in order:

1. In the terminal where `make up` last worked: `aws sts get-caller-identity`.
   If this also fails, your credentials themselves expired — refresh them
   there (re-export keys, or `aws sso login --profile <name>` if using SSO).
2. If that succeeds but the notebook still fails, the kernel's environment is
   what's stale. For SSO: run `!aws sso login --profile <name>` in a cell (see
   `03_msk_live_experiment.ipynb`'s second cell) — no restart needed, since
   AWS's tools re-read the SSO cache from disk on every call. For raw/temporary
   keys: set them in a cell via `getpass` rather than typing literal values
   into cell source, which `nbstripout` does not clean up:

   ```python
   import getpass
   import os

   os.environ["AWS_ACCESS_KEY_ID"] = getpass.getpass("AWS_ACCESS_KEY_ID: ")
   os.environ["AWS_SECRET_ACCESS_KEY"] = getpass.getpass("AWS_SECRET_ACCESS_KEY: ")
   os.environ["AWS_SESSION_TOKEN"] = getpass.getpass("AWS_SESSION_TOKEN (blank if none): ")
   ```

   If neither works, fully stop Jupyter (not just restart the kernel) and
   start it again — `make notebook` — from a terminal with valid credentials.

## Tests

`tests/devlab/` covers the pure logic. The pandas-dependent tests skip under a
plain `make test`; run the full set with:

```bash
make notebook-test
```
