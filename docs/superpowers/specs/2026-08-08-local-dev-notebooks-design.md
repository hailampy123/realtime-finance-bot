# Local dev notebooks and environment — design

**Status:** implemented.
**Scope:** an interactive local loop for working with the running trade
streams. Stage 1 is live and Stage 2 (Databricks) is design-only, so between
them there is no way to look at the data except running
`scripts/consume_example.py` and reading its stdout.

Related: [`2026-08-07-finance-data-ai-platform-design.md`](2026-08-07-finance-data-ai-platform-design.md)
(parent), [`2026-08-08-data-layer-batch-history-and-serving-design.md`](2026-08-08-data-layer-batch-history-and-serving-design.md)
(the Silver/Gold contracts this prototypes).

## 1. The problem, including the part that was not asked about

Three things to do interactively: watch stream health, explore the data, and
prototype the Stage 2 transforms before writing them as Structured Streaming.

Investigating turned up a precondition that blocks all three: **locally there
was no working way to start the streams at all.** `make compose-up` yields a
broker with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` and therefore no topics,
and the `producers` compose service cannot reach it — the broker advertises
`PLAINTEXT://localhost:9092`, which from inside a sibling container resolves to
that container. A notebook pointed at `localhost:9092` would have found nothing,
and the obvious diagnosis (broken notebook) would have been wrong.

So the deliverable includes a working local producer path. It does not fix the
container networking; running the producer on the host sidesteps the single
listener entirely, and adding a second listener is a change to the deployment
artefact for the benefit of the dev loop.

## 2. Decisions

| Decision | Choice | Why |
|---|---|---|
| Code layout | Thin notebooks over a `devlab/` package | Three notebooks would otherwise carry three copies of the consumer + SASL setup, drifting the moment the cluster is rebuilt. Logic in a package is lintable, typecheckable, and testable; logic in a notebook is none of those. |
| Dependency isolation | Non-default `notebook` group in the root pyproject | uv installs only the `dev` group by default, so `uv sync`, `make check`, and the image's `uv sync --frozen --no-dev` all skip it. One lockfile. A separate `notebooks/` uv project was considered and rejected: a second lockfile and a path dependency to maintain, for isolation that the group already provides. |
| Broker targets | Both, switchable via `$FDAI_TARGET` | Notebooks never name a hostname; flipping one variable moves all three. |
| Credentials | Reuse `ingest.settings.Settings` | `INGEST_*` already reads `.env`. A second config system would be a second thing to keep in sync. |
| Notebook format | `.ipynb`, outputs stripped | Zero barrier to opening. `nbstripout` keeps market data and JSON noise out of git. Ruff lints `.ipynb` code cells natively, so notebook code is held to the same standard. |
| DataFrame library | pandas | Ubiquitous, plots natively, and the transforms are being translated to PySpark regardless. |

## 3. Components

`devlab/` — four modules, none clever, all lint/typechecked alongside `ingest`.

- **`config`** — `Target` (name, bootstrap, SASL) and the resolvers `local()`,
  `msk()`, `from_terraform()`, `resolve()`. `local()` deliberately ignores
  `.env`: asking for local should give local, not a surprise round-trip to AWS.
  `from_terraform()` exists because broker DNS is regenerated on every
  `make up`, which makes a hardcoded `.env` entry actively misleading rather
  than merely stale. The password is excluded from `repr` — a bare `target` in
  a cell echoes its value, and notebooks get committed.
- **`stream`** — `tail()` / `collect()`. Decodes via the producer's own
  `trade_codec()`, and attaches `kafka_partition` / `kafka_offset` /
  `kafka_timestamp_ms` / `kafka_key` (the Avro schema has no `kafka_`-prefixed
  field, so collision is impossible).
- **`health`** — `topics()`, `partitions()`, `lag()`, `rate()`,
  `sequence_gaps()`. Returns dataclasses rather than DataFrames so it stays
  usable without pandas.
- **`frames`** — pandas conversion, plus `dedupe()` and `bars()`. The only
  module that imports pandas, and deliberately not re-exported from
  `devlab/__init__.py`.

`notebooks/` — `00_stream_health`, `01_explore_trades`, `02_prototype_silver`,
plus a README.

Make targets — `stream-local`, `notebook`, `notebook-test`, `notebook-clean`.
`typecheck` extends to `mypy ingest devlab`.

## 4. Three things worth arguing about

**Every read is bounded by both a count and a wall clock, and at least one must
be set.** `tail(limit=None, seconds=None)` raises. The characteristic failure
of Kafka-in-a-notebook is a cell that polls forever against a quiet topic,
which is indistinguishable from a broken broker; making it impossible beats
documenting it. Similarly, a missing topic raises within the metadata timeout
naming the command to run next, rather than polling nothing.

**Random consumer group per read, auto-commit off.** A stable group would
resume from a committed offset, so re-running a cell would show nothing and
read as a dead stream. Off-by-default committing also means `devlab` can never
move a real consumer's offsets.

**Coinbase is excluded from gap detection rather than reported clean.** Its
`sequence_num` is connection-wide (`CoinbaseConnector.sequence_symbol == "*"`)
while the topic is partitioned by `venue|symbol`. Kafka orders only within a
partition, so a connection-wide counter read back across six partitions is
interleaved by construction and every "gap" would be an artefact of the read.
Reporting Coinbase as clean would be a lie; checking it would be confident
nonsense. `GapReport.skipped_venues` names it.

A gap found this way is also weaker evidence than one from `IngestRunner`: it
means the record never reached Kafka *or* is no longer retained, and the runner
may already have repaired it at a later offset.

## 5. Testing

`tests/devlab/` covers the pure logic — target resolution and credential
redaction, read bounding and consumer cleanup, gap detection semantics, and the
frame transforms. Kafka I/O is faked; there is no new integration surface.

pandas-dependent tests use `pytest.importorskip`, so `make test` stays green
without the notebook group. `make notebook-test` runs the full set.

## 6. Verified against live data

Run end to end against `make stream-local` with both venues producing, rather
than only against fakes. Two findings, both from checks written into the
notebooks:

- **Dedupe dropped 37% of rows** (2166 → 1366). All duplicates were Coinbase,
  exactly two copies per natural key, one `STREAM` and one `REST_REPAIR`, with
  identical price, size, and timestamp. That is the documented best-effort
  repair path behaving as designed and `dedupe()` absorbing it — the data-layer
  spec's natural-key claim, demonstrated rather than asserted.
- **The VWAP sanity check was wrong.** It flagged 6 of 97 bars as having VWAP
  outside `low..high`. All six were flat bars (`high == low`), off by ~9e-16
  against a float64 eps scale of 1.4e-15 — `sum(p*s)/sum(s)` recovers `p` only
  to within an ulp. The check now carries a relative tolerance, with the
  reasoning inline, and a regression test pins the flat-bar case. It is also
  the concrete argument for `decimal(38, 18)` over `double` in Spark: the wire
  format keeps prices exact as strings, and Silver should not be where that
  quietly stops being true.

## 7. Deliberately not included

Connector-level debugging (replaying fixtures through `parse()`) — asked about,
declined. A Dockerised Jupyter service — it would hit the same advertised-listener
bug and pull in a compose Kafka listener change. Writing to Kafka from
notebooks — the dev loop is read-only; `ingest.cli` produces. Fixing the
`--profile live` container networking — still open, still documented in the
README's Known limitations.
