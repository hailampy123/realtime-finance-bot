# Foundation + Streaming Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a one-command-rebuildable AWS sandbox running Kafka, with resilient WebSocket producers streaming gap-repaired crypto trades from Binance and Coinbase into Avro-encoded Kafka topics.

**Architecture:** Terraform provisions an ephemeral account-A stack (VPC, MSK with SASL/SCRAM public access, one EC2 producer host). Python asyncio connectors read exchange WebSockets, normalize to a canonical `Trade` record, detect sequence gaps, repair them over REST, and publish Avro to Kafka. Schemas are files in git — no schema registry — so producer and consumer cannot drift.

**Tech Stack:** Python 3.12 · `uv` · `websockets` · `confluent-kafka` · `fastavro` · `pydantic` · `pytest` / `pytest-asyncio` · Terraform 1.9+ · AWS provider ~> 5.0 · Docker Compose · GitHub Actions

## Global Constraints

Copied verbatim from `docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`. Every task's requirements implicitly include these.

- **No IAM role creation** in the sandbox. No `aws_iam_role`, `aws_iam_role_policy`, or `aws_iam_instance_profile` resources may appear anywhere in `infra/`.
- Every IAM role ARN is a `var.*` input defaulting to `null`. Modules fall back to non-IAM auth when null.
- **Any manual console step is a bug.** If `make up` can't create it, it doesn't exist.
- **No secrets in git.** SASL passwords are generated at apply time and pushed to both Secrets Manager and the Databricks secret scope.
- **Trades are never dropped.** On queue saturation the producer blocks, takes the gap, and repairs it via REST. Depth updates may be dropped.
- Violations are **quarantined, never dropped**.
- `make up`: empty account to streaming data, **target under 20 minutes**.
- Python 3.12. Terraform 1.9+. AWS provider `~> 5.0`.
- Canonical price and size are transported as **strings**, not floats — lossless decimal.
- Canonical `Trade.side` is the **aggressor** side.

---

### Task 1: Repository foundation and CI

**Files:**
- Create: `pyproject.toml`, `.python-version`, `ingest/__init__.py`, `tests/__init__.py`, `.github/workflows/ci.yml`, `Makefile`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Consumes: nothing
- Produces: the `ingest` package importable as `ingest`; `make lint`, `make test`, `make typecheck` targets used by every later task.

- [ ] **Step 1: Write the failing test**

`tests/test_smoke.py`:

```python
def test_package_imports():
    import ingest

    assert ingest.__version__ == "0.1.0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_smoke.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest'`

- [ ] **Step 3: Create the package and config**

`.python-version`:

```text
3.12
```

`pyproject.toml`:

```toml
[project]
name = "finance-data-ai"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "websockets>=13.0",
    "confluent-kafka>=2.5.0",
    "fastavro>=1.9.0",
    "pydantic>=2.8",
    "pydantic-settings>=2.4",
    "pyyaml>=6.0",
    "httpx>=0.27",
    "structlog>=24.4",
]

[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "mypy>=1.11",
    "ruff>=0.6",
    "types-pyyaml>=6.0",
]

[tool.setuptools.packages.find]
include = ["ingest*", "backfill*"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC", "RUF"]

[tool.mypy]
python_version = "3.12"
strict = true
ignore_missing_imports = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

`ingest/__init__.py`:

```python
__version__ = "0.1.0"
```

`tests/__init__.py`: empty file.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv sync && uv run pytest tests/test_smoke.py -v`
Expected: PASS

- [ ] **Step 5: Add Makefile targets**

`Makefile`:

```makefile
.PHONY: lint test typecheck check

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy ingest

test:
	uv run pytest -v

check: lint typecheck test
```

- [ ] **Step 6: Add CI**

`.github/workflows/ci.yml`:

```yaml
name: ci
on: [push, pull_request]

jobs:
  python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy ingest
      - run: uv run pytest -v

  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: 1.9.8
      - run: terraform -chdir=infra/envs/dev init -backend=false
      - run: terraform -chdir=infra/envs/dev validate
      - run: terraform fmt -check -recursive infra/
```

- [ ] **Step 7: Verify the full check passes**

Run: `make check`
Expected: all three stages pass.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml .python-version uv.lock ingest tests Makefile .github
git commit -m "chore: repository foundation, tooling, and CI"
```

---

### Task 2: Avro schema and codec

**Files:**
- Create: `ingest/schemas/trade.v1.avsc`, `ingest/core/__init__.py`, `ingest/core/codec.py`
- Test: `tests/core/test_codec.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `TRADE_SCHEMA_VERSION: int = 1`
  - `class AvroCodec: __init__(self, schema_path: Path); encode(self, record: dict[str, Any]) -> bytes; decode(self, data: bytes) -> dict[str, Any]`
  - `def trade_codec(version: int = TRADE_SCHEMA_VERSION) -> AvroCodec`

- [ ] **Step 1: Write the failing test**

`tests/core/test_codec.py`:

```python
import pytest

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec

SAMPLE = {
    "venue": "binance",
    "venue_symbol": "BTCUSDT",
    "instrument_id": "BTC-USD",
    "trade_id": "12345",
    "event_ts_us": 1_700_000_000_000_000,
    "ingest_ts_us": 1_700_000_000_500_000,
    "price": "43210.55",
    "size": "0.0012",
    "side": "BUY",
    "sequence": 12345,
    "is_backfill": False,
    "source": "STREAM",
}


def test_roundtrip_preserves_all_fields():
    codec = trade_codec()
    assert codec.decode(codec.encode(SAMPLE)) == SAMPLE


def test_encoding_is_compact_binary():
    codec = trade_codec()
    encoded = codec.encode(SAMPLE)
    # schemaless_writer emits a bare datum: no embedded schema, no magic byte
    assert isinstance(encoded, bytes)
    assert len(encoded) < 120
    assert not encoded.startswith(b"Obj")


def test_price_survives_as_exact_string():
    codec = trade_codec()
    record = SAMPLE | {"price": "0.000000010000001"}
    assert codec.decode(codec.encode(record))["price"] == "0.000000010000001"


def test_unknown_version_raises():
    with pytest.raises(FileNotFoundError):
        trade_codec(version=99)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_codec.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core'`

- [ ] **Step 3: Write the schema**

`ingest/schemas/trade.v1.avsc`:

```json
{
  "type": "record",
  "name": "Trade",
  "namespace": "md.v1",
  "fields": [
    {"name": "venue", "type": "string"},
    {"name": "venue_symbol", "type": "string"},
    {"name": "instrument_id", "type": "string"},
    {"name": "trade_id", "type": "string"},
    {"name": "event_ts_us", "type": "long"},
    {"name": "ingest_ts_us", "type": "long"},
    {"name": "price", "type": "string"},
    {"name": "size", "type": "string"},
    {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL", "UNKNOWN"]}},
    {"name": "sequence", "type": ["null", "long"], "default": null},
    {"name": "is_backfill", "type": "boolean", "default": false},
    {"name": "source", "type": {"type": "enum", "name": "Source", "symbols": ["STREAM", "REST_REPAIR", "ARCHIVE"]}, "default": "STREAM"}
  ]
}
```

Price and size are strings so decimal values survive the wire byte-exact; Spark casts them to `DECIMAL` in Silver.

- [ ] **Step 4: Write the codec**

`ingest/core/__init__.py`: empty file.

`ingest/core/codec.py`:

```python
"""Avro encode/decode against schema files versioned in git.

There is no schema registry: the producer and the Spark reader load the same
.avsc file, so drift is impossible by construction. The wire format is a bare
Avro datum (fastavro schemaless_writer) with no Confluent magic-byte prefix,
which is exactly what Spark's from_avro(data, jsonFormatSchema) expects.
"""

from __future__ import annotations

import io
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import fastavro

TRADE_SCHEMA_VERSION = 1
SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"


class AvroCodec:
    def __init__(self, schema_path: Path) -> None:
        self.schema_path = schema_path
        raw = json.loads(schema_path.read_text())
        self.schema = fastavro.parse_schema(raw)

    def encode(self, record: dict[str, Any]) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.schema, record)
        return buf.getvalue()

    def decode(self, data: bytes) -> dict[str, Any]:
        result = fastavro.schemaless_reader(io.BytesIO(data), self.schema)
        return dict(result)


@lru_cache(maxsize=8)
def trade_codec(version: int = TRADE_SCHEMA_VERSION) -> AvroCodec:
    path = SCHEMA_DIR / f"trade.v{version}.avsc"
    if not path.exists():
        raise FileNotFoundError(f"no trade schema for version {version}: {path}")
    return AvroCodec(path)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/core/test_codec.py -v`
Expected: 4 passed

- [ ] **Step 6: Add the schema compatibility guard**

`tests/core/test_schema_compat.py`:

```python
"""A breaking schema change must fail CI, not a 3am streaming job."""

import json

import fastavro
import pytest

from ingest.core.codec import SCHEMA_DIR, TRADE_SCHEMA_VERSION, trade_codec

SAMPLE = {
    "venue": "binance",
    "venue_symbol": "BTCUSDT",
    "instrument_id": "BTC-USD",
    "trade_id": "12345",
    "event_ts_us": 1_700_000_000_000_000,
    "ingest_ts_us": 1_700_000_000_500_000,
    "price": "43210.55",
    "size": "0.0012",
    "side": "BUY",
    "sequence": 12345,
    "is_backfill": False,
    "source": "STREAM",
}


def test_every_schema_file_parses():
    for path in SCHEMA_DIR.glob("*.avsc"):
        fastavro.parse_schema(json.loads(path.read_text()))


@pytest.mark.parametrize("older", range(1, TRADE_SCHEMA_VERSION))
def test_current_reader_can_read_older_writer(older: int):
    """Data written by an older producer must still decode with today's schema."""
    old_codec = trade_codec(version=older)
    new_schema = trade_codec().schema
    encoded = old_codec.encode(SAMPLE)
    import io

    decoded = fastavro.schemaless_reader(io.BytesIO(encoded), old_codec.schema, new_schema)
    assert decoded["venue"] == "binance"


def test_added_fields_must_have_defaults():
    """Any field added after v1 needs a default, or old data becomes unreadable."""
    v1 = json.loads((SCHEMA_DIR / "trade.v1.avsc").read_text())
    v1_names = {f["name"] for f in v1["fields"]}
    current = json.loads((SCHEMA_DIR / f"trade.v{TRADE_SCHEMA_VERSION}.avsc").read_text())
    for field in current["fields"]:
        if field["name"] not in v1_names:
            assert "default" in field, f"new field {field['name']} needs a default"
```

- [ ] **Step 7: Run the compatibility tests**

Run: `uv run pytest tests/core/test_schema_compat.py -v`
Expected: PASS (the parametrized test collects zero cases at v1 — that is correct and it starts guarding at v2)

- [ ] **Step 8: Commit**

```bash
git add ingest/schemas ingest/core tests/core
git commit -m "feat: Avro trade schema and registry-free codec with compat guard"
```

---

### Task 3: Canonical domain model and instrument mapping

**Files:**
- Create: `ingest/core/models.py`, `config/universe.yaml`, `ingest/core/instruments.py`
- Test: `tests/core/test_models.py`, `tests/core/test_instruments.py`

**Interfaces:**
- Consumes: `trade_codec` from Task 2
- Produces:
  - `class Side(StrEnum)` with `BUY`, `SELL`, `UNKNOWN`
  - `class Source(StrEnum)` with `STREAM`, `REST_REPAIR`, `ARCHIVE`
  - `@dataclass(frozen=True, slots=True) class Trade` with fields matching the Avro schema, plus `kafka_key() -> bytes` and `to_avro() -> dict[str, Any]`
  - `class InstrumentMap: from_yaml(path: Path) -> InstrumentMap; canonical(venue: str, venue_symbol: str) -> str; symbols_for(venue: str) -> list[str]`

- [ ] **Step 1: Write the failing test**

`tests/core/test_models.py`:

```python
from ingest.core.codec import trade_codec
from ingest.core.models import Side, Source, Trade


def make_trade(**overrides) -> Trade:
    base = dict(
        venue="binance",
        venue_symbol="BTCUSDT",
        instrument_id="BTC-USD",
        trade_id="12345",
        event_ts_us=1_700_000_000_000_000,
        ingest_ts_us=1_700_000_000_500_000,
        price="43210.55",
        size="0.0012",
        side=Side.BUY,
        sequence=12345,
        is_backfill=False,
        source=Source.STREAM,
    )
    return Trade(**(base | overrides))


def test_kafka_key_is_venue_pipe_symbol():
    assert make_trade().kafka_key() == b"binance|BTCUSDT"


def test_kafka_key_groups_same_instrument_same_venue():
    a = make_trade(trade_id="1")
    b = make_trade(trade_id="2")
    assert a.kafka_key() == b.kafka_key()


def test_kafka_key_separates_venues():
    assert make_trade(venue="coinbase").kafka_key() != make_trade().kafka_key()


def test_to_avro_roundtrips_through_the_codec():
    codec = trade_codec()
    trade = make_trade()
    assert codec.decode(codec.encode(trade.to_avro())) == trade.to_avro()


def test_trade_is_immutable():
    import dataclasses

    import pytest

    with pytest.raises(dataclasses.FrozenInstanceError):
        make_trade().price = "1"  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.models'`

- [ ] **Step 3: Write the model**

`ingest/core/models.py`:

```python
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    """Aggressor side — the side that crossed the spread."""

    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class Source(StrEnum):
    STREAM = "STREAM"
    REST_REPAIR = "REST_REPAIR"
    ARCHIVE = "ARCHIVE"


@dataclass(frozen=True, slots=True)
class Trade:
    venue: str
    venue_symbol: str
    instrument_id: str
    trade_id: str
    event_ts_us: int
    ingest_ts_us: int
    price: str
    size: str
    side: Side
    sequence: int | None
    is_backfill: bool
    source: Source

    def kafka_key(self) -> bytes:
        """Key on venue|symbol so one instrument on one venue stays ordered."""
        return f"{self.venue}|{self.venue_symbol}".encode()

    def to_avro(self) -> dict[str, Any]:
        record = asdict(self)
        record["side"] = str(self.side)
        record["source"] = str(self.source)
        return record
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_models.py -v`
Expected: 5 passed

- [ ] **Step 5: Write the instrument-map test**

`tests/core/test_instruments.py`:

```python
import pytest

from ingest.core.instruments import InstrumentMap

YAML = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues:
      binance: BTCUSDT
      coinbase: BTC-USD
  - id: ETH-USD
    asset_class: crypto
    venues:
      binance: ETHUSDT
      coinbase: ETH-USD
"""


@pytest.fixture
def imap(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(YAML)
    return InstrumentMap.from_yaml(path)


def test_collapses_venue_symbols_to_one_canonical_id(imap):
    assert imap.canonical("binance", "BTCUSDT") == "BTC-USD"
    assert imap.canonical("coinbase", "BTC-USD") == "BTC-USD"


def test_symbols_for_returns_that_venues_tickers(imap):
    assert sorted(imap.symbols_for("binance")) == ["BTCUSDT", "ETHUSDT"]


def test_unknown_symbol_raises_rather_than_guessing(imap):
    with pytest.raises(KeyError, match="DOGEUSDT"):
        imap.canonical("binance", "DOGEUSDT")
```

- [ ] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/core/test_instruments.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.instruments'`

- [ ] **Step 7: Write the instrument map and the universe config**

`ingest/core/instruments.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class InstrumentMap:
    """Maps venue-specific tickers onto one canonical instrument id.

    BTCUSDT (binance) and BTC-USD (coinbase) are the same instrument; Silver
    joins on the canonical id, so the mapping has to live upstream of Kafka.
    """

    _to_canonical: dict[tuple[str, str], str]
    _by_venue: dict[str, list[str]]

    @classmethod
    def from_yaml(cls, path: Path) -> InstrumentMap:
        doc = yaml.safe_load(path.read_text())
        to_canonical: dict[tuple[str, str], str] = {}
        by_venue: dict[str, list[str]] = {}
        for entry in doc["instruments"]:
            for venue, venue_symbol in entry["venues"].items():
                to_canonical[(venue, venue_symbol)] = entry["id"]
                by_venue.setdefault(venue, []).append(venue_symbol)
        return cls(to_canonical, by_venue)

    def canonical(self, venue: str, venue_symbol: str) -> str:
        try:
            return self._to_canonical[(venue, venue_symbol)]
        except KeyError:
            raise KeyError(f"unmapped instrument {venue}:{venue_symbol}") from None

    def symbols_for(self, venue: str) -> list[str]:
        return list(self._by_venue.get(venue, []))
```

`config/universe.yaml`:

```yaml
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT, coinbase: BTC-USD}
  - id: ETH-USD
    asset_class: crypto
    venues: {binance: ETHUSDT, coinbase: ETH-USD}
  - id: SOL-USD
    asset_class: crypto
    venues: {binance: SOLUSDT, coinbase: SOL-USD}
  - id: XRP-USD
    asset_class: crypto
    venues: {binance: XRPUSDT, coinbase: XRP-USD}
  - id: ADA-USD
    asset_class: crypto
    venues: {binance: ADAUSDT, coinbase: ADA-USD}
  - id: LINK-USD
    asset_class: crypto
    venues: {binance: LINKUSDT, coinbase: LINK-USD}
  - id: AVAX-USD
    asset_class: crypto
    venues: {binance: AVAXUSDT, coinbase: AVAX-USD}
  - id: DOGE-USD
    asset_class: crypto
    venues: {binance: DOGEUSDT, coinbase: DOGE-USD}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/core -v`
Expected: all pass

- [ ] **Step 9: Commit**

```bash
git add ingest/core/models.py ingest/core/instruments.py config/universe.yaml tests/core
git commit -m "feat: canonical Trade model and venue-symbol instrument mapping"
```

---

### Task 4: Token-bucket rate limiter

**Files:**
- Create: `ingest/core/ratelimit.py`
- Test: `tests/core/test_ratelimit.py`

**Interfaces:**
- Consumes: nothing
- Produces: `class TokenBucket: __init__(self, rate_per_sec: float, capacity: float, *, now: Callable[[], float] = time.monotonic); async acquire(self, tokens: float = 1.0) -> None; def try_acquire(self, tokens: float = 1.0) -> bool`
- Produces: `VENUE_LIMITS: dict[str, TokenBucket]` factory `def bucket_for(venue: str) -> TokenBucket`

- [ ] **Step 1: Write the failing test**

`tests/core/test_ratelimit.py`:

```python
import asyncio

import pytest

from ingest.core.ratelimit import TokenBucket, bucket_for


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_burst_up_to_capacity_then_refuses():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=1.0, capacity=3.0, now=clock)
    assert [bucket.try_acquire() for _ in range(3)] == [True, True, True]
    assert bucket.try_acquire() is False


def test_refills_at_the_configured_rate():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=2.0, capacity=2.0, now=clock)
    bucket.try_acquire(2.0)
    assert bucket.try_acquire() is False
    clock.advance(0.5)  # 0.5s * 2/s = 1 token
    assert bucket.try_acquire() is True


def test_refill_never_exceeds_capacity():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=10.0, capacity=2.0, now=clock)
    clock.advance(100.0)
    assert bucket.try_acquire(2.0) is True
    assert bucket.try_acquire() is False


async def test_acquire_waits_instead_of_failing():
    bucket = TokenBucket(rate_per_sec=100.0, capacity=1.0)
    await bucket.acquire()
    await asyncio.wait_for(bucket.acquire(), timeout=1.0)


def test_known_venues_have_conservative_limits():
    assert bucket_for("kraken").rate_per_sec <= 1.0
    assert bucket_for("coinbase").rate_per_sec <= 10.0
    with pytest.raises(KeyError):
        bucket_for("nasdaq-direct-feed")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_ratelimit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.ratelimit'`

- [ ] **Step 3: Write the implementation**

`ingest/core/ratelimit.py`:

```python
"""Per-venue token buckets.

REST is only used for gap repair and snapshots, so the budget is small — but
exceeding a venue's published limit gets the source IP banned, which on a
weekly-rebuilt sandbox is a genuinely painful failure mode.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from functools import lru_cache


class TokenBucket:
    def __init__(
        self,
        rate_per_sec: float,
        capacity: float,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity
        self._now = now
        self._tokens = capacity
        self._updated = now()

    def _refill(self) -> None:
        current = self._now()
        elapsed = current - self._updated
        self._updated = current
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> None:
        while not self try_acquire(tokens):  # placeholder-guard: replaced below
            pass


# Published public-endpoint limits, deliberately set below the documented ceiling.
_RATES: dict[str, tuple[float, float]] = {
    "binance": (10.0, 20.0),
    "coinbase": (8.0, 10.0),
    "kraken": (1.0, 1.0),
}


@lru_cache(maxsize=16)
def bucket_for(venue: str) -> TokenBucket:
    if venue not in _RATES:
        raise KeyError(f"no rate limit configured for venue {venue!r}")
    rate, capacity = _RATES[venue]
    return TokenBucket(rate_per_sec=rate, capacity=capacity)
```

- [ ] **Step 4: Fix the deliberately broken `acquire`**

The `acquire` body above is a syntax error and a busy-wait. Replace it with:

```python
    async def acquire(self, tokens: float = 1.0) -> None:
        while not self.try_acquire(tokens):
            self._refill()
            deficit = tokens - self._tokens
            await asyncio.sleep(max(deficit / self.rate_per_sec, 0.001))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_ratelimit.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add ingest/core/ratelimit.py tests/core/test_ratelimit.py
git commit -m "feat: per-venue token-bucket rate limiting"
```

---

### Task 5: Resilient WebSocket client

**Files:**
- Create: `ingest/core/ws.py`
- Test: `tests/core/test_ws.py`

**Interfaces:**
- Consumes: nothing
- Produces: `class ResilientWebSocket: __init__(self, url: str, subscribe: list[dict[str, Any]], on_message: Callable[[str], Awaitable[None]], on_reconnect: Callable[[], Awaitable[None]] | None = None, *, max_lifetime_s: float = 23 * 3600, max_backoff_s: float = 60.0, connect: ConnectFn | None = None); async run(self, stop: asyncio.Event) -> None`
- Produces: `def backoff_delays(max_backoff_s: float, jitter: Callable[[], float]) -> Iterator[float]`

- [ ] **Step 1: Write the failing test**

`tests/core/test_ws.py`:

```python
import asyncio
import json

import pytest

from ingest.core.ws import ResilientWebSocket, backoff_delays


def test_backoff_grows_exponentially_and_caps():
    delays = list(itertools_take(backoff_delays(max_backoff_s=30.0, jitter=lambda: 0.0), 8))
    assert delays[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert all(d <= 30.0 for d in delays)


def itertools_take(iterator, n):
    return [next(iterator) for _ in range(n)]


def test_backoff_applies_jitter():
    delays = itertools_take(backoff_delays(max_backoff_s=30.0, jitter=lambda: 0.5), 3)
    assert delays == [1.5, 3.0, 6.0]


class FakeSocket:
    """Yields a scripted list of frames, then raises to simulate a drop."""

    def __init__(self, frames: list[str], fail_after: bool = True) -> None:
        self.frames = frames
        self.fail_after = fail_after
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for frame in self.frames:
            yield frame
        if self.fail_after:
            raise ConnectionResetError("simulated drop")


async def test_delivers_frames_to_on_message():
    received: list[str] = []
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}', '{"a":2}'], fail_after=False)]

    async def connect(url: str):
        return sockets.pop(0)

    async def on_message(raw: str) -> None:
        received.append(raw)
        if len(received) == 2:
            stop.set()

    ws = ResilientWebSocket("wss://x", [{"sub": 1}], on_message, connect=connect)
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert received == ['{"a":1}', '{"a":2}']


async def test_sends_subscribe_payload_on_every_connection():
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}']), FakeSocket(['{"a":2}'], fail_after=False)]

    async def connect(url: str):
        return sockets.pop(0)

    seen: list[str] = []

    async def on_message(raw: str) -> None:
        seen.append(raw)
        if len(seen) == 2:
            stop.set()

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, connect=connect, max_backoff_s=0.01
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert json.loads(sockets_sent(ws)[0]) == {"sub": 1}


def sockets_sent(ws):
    return ws.last_sent


async def test_reconnect_hook_fires_after_a_drop():
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}']), FakeSocket(['{"a":2}'], fail_after=False)]
    reconnects: list[int] = []

    async def connect(url: str):
        return sockets.pop(0)

    seen: list[str] = []

    async def on_message(raw: str) -> None:
        seen.append(raw)
        if len(seen) == 2:
            stop.set()

    async def on_reconnect() -> None:
        reconnects.append(1)

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, on_reconnect, connect=connect, max_backoff_s=0.01
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert len(reconnects) == 1, "the hook must fire on the reconnect, not the first connect"


async def test_stop_event_ends_the_loop():
    stop = asyncio.Event()
    stop.set()

    async def connect(url: str):
        pytest.fail("must not connect once stop is already set")

    ws = ResilientWebSocket("wss://x", [], lambda raw: asyncio.sleep(0), connect=connect)
    await asyncio.wait_for(ws.run(stop), timeout=1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_ws.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.ws'`

- [ ] **Step 3: Write the implementation**

`ingest/core/ws.py`:

```python
"""A WebSocket connection that assumes it will be dropped, because it will be.

Three behaviours matter here and none are default:
  * exponential backoff with jitter, so a venue outage does not become a
    thundering-herd reconnect storm;
  * proactive reconnect before the venue's own connection lifetime expires
    (Binance closes at 24h) — reconnecting on our schedule beats being dropped;
  * an on_reconnect hook, which is where gap repair is triggered.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Protocol

import structlog

log = structlog.get_logger(__name__)


class Socket(Protocol):
    async def send(self, payload: str) -> None: ...
    def __aiter__(self) -> AsyncIterator[str]: ...
    async def __aenter__(self) -> Socket: ...
    async def __aexit__(self, *exc: object) -> bool: ...


ConnectFn = Callable[[str], Awaitable[Socket]]


def backoff_delays(max_backoff_s: float, jitter: Callable[[], float]) -> Iterator[float]:
    """1s, 2s, 4s ... capped, each multiplied by (1 + jitter())."""
    delay = 1.0
    while True:
        yield min(delay * (1.0 + jitter()), max_backoff_s)
        delay = min(delay * 2.0, max_backoff_s)


async def _default_connect(url: str) -> Socket:
    import websockets

    return await websockets.connect(url, ping_interval=20, ping_timeout=20)  # type: ignore[return-value]


class ResilientWebSocket:
    def __init__(
        self,
        url: str,
        subscribe: list[dict[str, Any]],
        on_message: Callable[[str], Awaitable[None]],
        on_reconnect: Callable[[], Awaitable[None]] | None = None,
        *,
        max_lifetime_s: float = 23 * 3600,
        max_backoff_s: float = 60.0,
        connect: ConnectFn | None = None,
    ) -> None:
        self.url = url
        self.subscribe = subscribe
        self.on_message = on_message
        self.on_reconnect = on_reconnect
        self.max_lifetime_s = max_lifetime_s
        self.max_backoff_s = max_backoff_s
        self._connect = connect or _default_connect
        self.last_sent: list[str] = []

    async def run(self, stop: asyncio.Event) -> None:
        delays = backoff_delays(self.max_backoff_s, lambda: random.random() * 0.3)
        first_connection = True
        while not stop.is_set():
            try:
                await self._session(stop, first_connection)
                delays = backoff_delays(self.max_backoff_s, lambda: random.random() * 0.3)
            except Exception as exc:  # noqa: BLE001 — any failure means reconnect
                log.warning("ws_session_failed", url=self.url, error=str(exc))
            first_connection = False
            if stop.is_set():
                return
            await asyncio.sleep(next(delays))

    async def _session(self, stop: asyncio.Event, first_connection: bool) -> None:
        socket = await self._connect(self.url)
        async with socket:
            for payload in self.subscribe:
                encoded = json.dumps(payload)
                self.last_sent.append(encoded)
                await socket.send(encoded)
            if not first_connection and self.on_reconnect is not None:
                await self.on_reconnect()
            deadline = asyncio.get_running_loop().time() + self.max_lifetime_s
            async for raw in socket:
                await self.on_message(raw)
                if stop.is_set():
                    return
                if asyncio.get_running_loop().time() >= deadline:
                    log.info("ws_proactive_reconnect", url=self.url)
                    return
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_ws.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add ingest/core/ws.py tests/core/test_ws.py
git commit -m "feat: resilient WebSocket client with jittered backoff and proactive reconnect"
```

---

### Task 6: Bounded queue with per-topic drop policy

**Files:**
- Create: `ingest/core/queue.py`
- Test: `tests/core/test_queue.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `class DropPolicy(StrEnum)` with `BLOCK`, `DROP_OLDEST`
  - `class BoundedTopicQueue: __init__(self, maxsize: int, policy: DropPolicy); async put(self, item: T) -> None; async get(self) -> T; def qsize(self) -> int; @property dropped: int`
  - `TOPIC_POLICIES: dict[str, DropPolicy]`

- [ ] **Step 1: Write the failing test**

`tests/core/test_queue.py`:

```python
import asyncio

import pytest

from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue, DropPolicy


async def test_drop_oldest_evicts_and_counts():
    q = BoundedTopicQueue(maxsize=2, policy=DropPolicy.DROP_OLDEST)
    await q.put("a")
    await q.put("b")
    await q.put("c")
    assert q.qsize() == 2
    assert q.dropped == 1
    assert await q.get() == "b"
    assert await q.get() == "c"


async def test_block_policy_waits_for_room_and_never_drops():
    q = BoundedTopicQueue(maxsize=1, policy=DropPolicy.BLOCK)
    await q.put("a")
    put_task = asyncio.create_task(q.put("b"))
    await asyncio.sleep(0)
    assert not put_task.done(), "BLOCK must not complete while the queue is full"
    assert await q.get() == "a"
    await asyncio.wait_for(put_task, timeout=1.0)
    assert await q.get() == "b"
    assert q.dropped == 0


def test_trades_block_and_depth_drops():
    """The one failure this system must not have is silent trade loss."""
    assert TOPIC_POLICIES["md.trades.v1"] is DropPolicy.BLOCK
    assert TOPIC_POLICIES["md.book.depth.v1"] is DropPolicy.DROP_OLDEST


def test_every_configured_topic_has_an_explicit_policy():
    for topic, policy in TOPIC_POLICIES.items():
        assert isinstance(policy, DropPolicy), topic


async def test_get_waits_when_empty():
    q = BoundedTopicQueue(maxsize=1, policy=DropPolicy.BLOCK)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.05)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_queue.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.queue'`

- [ ] **Step 3: Write the implementation**

`ingest/core/queue.py`:

```python
"""Backpressure policy, stated per topic rather than left to emerge.

Depth updates are recoverable from the next snapshot, so they may be dropped.
Trades are not recoverable from anything cheaper than a REST call, so the
producer blocks, takes the gap, and repairs it. Silent trade loss is the one
failure this system must not have.
"""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import Generic, TypeVar

import structlog

log = structlog.get_logger(__name__)

T = TypeVar("T")


class DropPolicy(StrEnum):
    BLOCK = "BLOCK"
    DROP_OLDEST = "DROP_OLDEST"


TOPIC_POLICIES: dict[str, DropPolicy] = {
    "md.trades.v1": DropPolicy.BLOCK,
    "md.bars.v1": DropPolicy.BLOCK,
    "md.book.top.v1": DropPolicy.DROP_OLDEST,
    "md.book.depth.v1": DropPolicy.DROP_OLDEST,
    "news.articles.v1": DropPolicy.BLOCK,
    "ops.metrics.v1": DropPolicy.DROP_OLDEST,
}


class BoundedTopicQueue(Generic[T]):
    def __init__(self, maxsize: int, policy: DropPolicy) -> None:
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self._policy = policy
        self._dropped = 0

    @property
    def dropped(self) -> int:
        return self._dropped

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, item: T) -> None:
        if self._policy is DropPolicy.BLOCK:
            await self._queue.put(item)
            return
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()
                    self._dropped += 1
                    log.warning("queue_dropped_oldest", total_dropped=self._dropped)
                except asyncio.QueueEmpty:  # pragma: no cover - racy shrink
                    continue

    async def get(self) -> T:
        return await self._queue.get()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_queue.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add ingest/core/queue.py tests/core/test_queue.py
git commit -m "feat: bounded queue with explicit per-topic backpressure policy"
```

---

### Task 7: Kafka producer wrapper

**Files:**
- Create: `ingest/core/producer.py`
- Test: `tests/core/test_producer.py`

**Interfaces:**
- Consumes: `Trade` (Task 3), `trade_codec`, `TRADE_SCHEMA_VERSION` (Task 2)
- Produces: `class TradeProducer: __init__(self, bootstrap_servers: str, *, sasl_username: str | None = None, sasl_password: str | None = None, producer_factory: Callable[[dict[str, Any]], Any] | None = None); produce(self, topic: str, trade: Trade) -> None; poll(self, timeout: float = 0.0) -> int; flush(self, timeout: float = 10.0) -> int`
- Produces: `def build_config(bootstrap_servers, sasl_username, sasl_password) -> dict[str, Any]`

- [ ] **Step 1: Write the failing test**

`tests/core/test_producer.py`:

```python
from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade
from ingest.core.producer import TradeProducer, build_config

TRADE = Trade(
    venue="binance",
    venue_symbol="BTCUSDT",
    instrument_id="BTC-USD",
    trade_id="12345",
    event_ts_us=1_700_000_000_000_000,
    ingest_ts_us=1_700_000_000_500_000,
    price="43210.55",
    size="0.0012",
    side=Side.BUY,
    sequence=12345,
    is_backfill=False,
    source=Source.STREAM,
)


class FakeKafka:
    def __init__(self, config):
        self.config = config
        self.produced: list[dict] = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": dict(headers or [])}
        )

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def make_producer() -> tuple[TradeProducer, FakeKafka]:
    holder: list[FakeKafka] = []

    def factory(config):
        holder.append(FakeKafka(config))
        return holder[-1]

    producer = TradeProducer("broker:9096", producer_factory=factory)
    return producer, holder[0]


def test_config_enables_idempotence_and_full_acks():
    config = build_config("broker:9096", None, None)
    assert config["enable.idempotence"] is True
    assert str(config["acks"]) == "all"


def test_sasl_is_configured_only_when_credentials_are_supplied():
    plain = build_config("broker:9096", None, None)
    assert "sasl.mechanisms" not in plain

    secured = build_config("broker:9096", "user", "pass")
    assert secured["security.protocol"] == "SASL_SSL"
    assert secured["sasl.mechanisms"] == "SCRAM-SHA-512"
    assert secured["sasl.username"] == "user"


def test_produce_uses_venue_symbol_key():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    assert kafka.produced[0]["key"] == b"binance|BTCUSDT"


def test_produce_writes_decodable_avro():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    decoded = trade_codec().decode(kafka.produced[0]["value"])
    assert decoded["trade_id"] == "12345"
    assert decoded["price"] == "43210.55"


def test_schema_version_travels_as_a_header():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    headers = kafka.produced[0]["headers"]
    assert headers["schema_version"] == str(TRADE_SCHEMA_VERSION).encode()
    assert headers["is_backfill"] == b"false"


def test_backfill_flag_reaches_the_header():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", Trade(**{**TRADE.__dict__, "is_backfill": True}))
    assert kafka.produced[0]["headers"]["is_backfill"] == b"true"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_producer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.producer'`

- [ ] **Step 3: Write the implementation**

`ingest/core/producer.py`:

```python
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import structlog

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Trade

log = structlog.get_logger(__name__)


def build_config(
    bootstrap_servers: str,
    sasl_username: str | None,
    sasl_password: str | None,
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 20,
        "batch.size": 262_144,
        "max.in.flight.requests.per.connection": 5,
        "retries": 10_000_000,
        "delivery.timeout.ms": 300_000,
    }
    if sasl_username and sasl_password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
        }
    return config


def _default_factory(config: dict[str, Any]) -> Any:
    from confluent_kafka import Producer

    return Producer(config)


class TradeProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        *,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        factory = producer_factory or _default_factory
        self._producer = factory(build_config(bootstrap_servers, sasl_username, sasl_password))
        self._codec = trade_codec()

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            log.error("kafka_delivery_failed", error=str(err))

    def produce(self, topic: str, trade: Trade) -> None:
        self._producer.produce(
            topic,
            key=trade.kafka_key(),
            value=self._codec.encode(trade.to_avro()),
            headers=[
                ("schema_version", str(TRADE_SCHEMA_VERSION).encode()),
                ("venue", trade.venue.encode()),
                ("is_backfill", b"true" if trade.is_backfill else b"false"),
                ("source", str(trade.source).encode()),
            ],
            on_delivery=self._on_delivery,
        )

    def poll(self, timeout: float = 0.0) -> int:
        return int(self._producer.poll(timeout))

    def flush(self, timeout: float = 10.0) -> int:
        return int(self._producer.flush(timeout))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_producer.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add ingest/core/producer.py tests/core/test_producer.py
git commit -m "feat: idempotent Avro Kafka producer with schema-version headers"
```

---

### Task 8: Sequence gap tracker

**Files:**
- Create: `ingest/core/gaps.py`
- Test: `tests/core/test_gaps.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `@dataclass(frozen=True) class Gap: venue: str; venue_symbol: str; last_seen: int; next_seen: int; @property missing_count: int`
  - `class SequenceTracker: observe(self, venue: str, venue_symbol: str, sequence: int | None) -> Gap | None; def reset(self, venue: str, venue_symbol: str) -> None`

- [ ] **Step 1: Write the failing test**

`tests/core/test_gaps.py`:

```python
from ingest.core.gaps import Gap, SequenceTracker


def test_first_observation_is_never_a_gap():
    tracker = SequenceTracker()
    assert tracker.observe("binance", "BTCUSDT", 100) is None


def test_contiguous_sequences_report_no_gap():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    assert tracker.observe("binance", "BTCUSDT", 101) is None


def test_jump_reports_a_gap_with_the_missing_range():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    gap = tracker.observe("binance", "BTCUSDT", 105)
    assert gap == Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    assert gap.missing_count == 4


def test_symbols_are_tracked_independently():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    tracker.observe("binance", "ETHUSDT", 900)
    assert tracker.observe("binance", "BTCUSDT", 101) is None
    assert tracker.observe("binance", "ETHUSDT", 901) is None


def test_venues_are_tracked_independently():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTC-USD", 100)
    assert tracker.observe("coinbase", "BTC-USD", 5) is None


def test_out_of_order_or_replayed_sequences_are_not_gaps():
    """REST repair republishes older ids; that must not look like a new gap."""
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    assert tracker.observe("binance", "BTCUSDT", 98) is None
    assert tracker.observe("binance", "BTCUSDT", 100) is None
    assert tracker.observe("binance", "BTCUSDT", 101) is None


def test_none_sequence_is_ignored():
    """Kraken publishes no stable id; absence of a sequence is not a gap."""
    tracker = SequenceTracker()
    assert tracker.observe("kraken", "XBT/USD", None) is None
    assert tracker.observe("kraken", "XBT/USD", None) is None


def test_reset_forgets_the_watermark():
    tracker = SequenceTracker()
    tracker.observe("binance", "BTCUSDT", 100)
    tracker.reset("binance", "BTCUSDT")
    assert tracker.observe("binance", "BTCUSDT", 5000) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_gaps.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.core.gaps'`

- [ ] **Step 3: Write the implementation**

`ingest/core/gaps.py`:

```python
"""Sequence-gap detection.

A WebSocket that silently misses forty seconds of trades is the classic
market-data failure, and it is invisible unless something is explicitly
watching the sequence numbers.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Gap:
    venue: str
    venue_symbol: str
    last_seen: int
    next_seen: int

    @property
    def missing_count(self) -> int:
        return self.next_seen - self.last_seen - 1


class SequenceTracker:
    def __init__(self) -> None:
        self._watermarks: dict[tuple[str, str], int] = {}

    def observe(self, venue: str, venue_symbol: str, sequence: int | None) -> Gap | None:
        if sequence is None:
            return None
        key = (venue, venue_symbol)
        last = self._watermarks.get(key)
        self._watermarks[key] = max(sequence, last) if last is not None else sequence
        if last is None or sequence <= last + 1:
            return None
        gap = Gap(venue=venue, venue_symbol=venue_symbol, last_seen=last, next_seen=sequence)
        log.warning(
            "gap_detected",
            venue=venue,
            symbol=venue_symbol,
            missing=gap.missing_count,
            last_seen=last,
            next_seen=sequence,
        )
        return gap

    def reset(self, venue: str, venue_symbol: str) -> None:
        self._watermarks.pop((venue, venue_symbol), None)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/core/test_gaps.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add ingest/core/gaps.py tests/core/test_gaps.py
git commit -m "feat: per-venue-per-symbol sequence gap detection"
```

---

### Task 9: Binance connector

**Files:**
- Create: `ingest/connectors/__init__.py`, `ingest/connectors/base.py`, `ingest/connectors/binance.py`
- Create: `tests/fixtures/binance_aggtrade.json`
- Test: `tests/connectors/test_binance.py`

**Interfaces:**
- Consumes: `Trade`, `Side`, `Source` (Task 3), `InstrumentMap` (Task 3), `Gap` (Task 8)
- Produces:
  - `class Connector(Protocol): venue: str; def stream_url(self, symbols: list[str]) -> str; def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]; def parse(self, raw: str) -> list[Trade]; async def repair(self, gap: Gap) -> list[Trade]`
  - `class BinanceConnector(Connector)` with `venue = "binance"`

**Why aggTrade rather than trade:** the raw `@trade` stream's ids can only be
backfilled through `/api/v3/historicalTrades`, which requires an API key. The
`@aggTrade` stream's `a` id shares an id space with the keyless
`/api/v3/aggTrades?fromId=` endpoint, so exact range repair works with no
credentials. Lower volume too.

- [ ] **Step 1: Record the fixture**

`tests/fixtures/binance_aggtrade.json` — a real frame from the combined stream:

```json
{
  "stream": "btcusdt@aggTrade",
  "data": {
    "e": "aggTrade",
    "E": 1700000000123,
    "s": "BTCUSDT",
    "a": 987654321,
    "p": "43210.55000000",
    "q": "0.00120000",
    "f": 111111,
    "l": 111113,
    "T": 1700000000100,
    "m": true,
    "M": true
  }
}
```

- [ ] **Step 2: Write the failing test**

`tests/connectors/test_binance.py`:

```python
import json
from pathlib import Path

import pytest

from ingest.connectors.binance import BinanceConnector
from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source

FIXTURE = Path(__file__).parent.parent / "fixtures" / "binance_aggtrade.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT}
  - id: ETH-USD
    asset_class: crypto
    venues: {binance: ETHUSDT}
"""


@pytest.fixture
def connector(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    return BinanceConnector(InstrumentMap.from_yaml(path))


def test_stream_url_lowercases_and_joins_symbols(connector):
    url = connector.stream_url(["BTCUSDT", "ETHUSDT"])
    assert url == (
        "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"
    )


def test_binance_needs_no_subscribe_frame(connector):
    assert connector.subscribe_payloads(["BTCUSDT"]) == []


def test_parses_the_recorded_frame(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.venue == "binance"
    assert trade.venue_symbol == "BTCUSDT"
    assert trade.instrument_id == "BTC-USD"
    assert trade.trade_id == "987654321"
    assert trade.sequence == 987654321
    assert trade.price == "43210.55000000"
    assert trade.size == "0.00120000"
    assert trade.source is Source.STREAM
    assert trade.is_backfill is False


def test_event_time_converts_milliseconds_to_microseconds(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.event_ts_us == 1_700_000_000_100_000


def test_buyer_maker_true_means_the_seller_was_the_aggressor(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.side is Side.SELL


def test_buyer_maker_false_means_the_buyer_was_the_aggressor(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["data"]["m"] = False
    (trade,) = connector.parse(json.dumps(frame))
    assert trade.side is Side.BUY


def test_non_trade_frames_are_ignored(connector):
    assert connector.parse(json.dumps({"result": None, "id": 1})) == []


def test_unmapped_symbol_is_skipped_not_crashed(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["data"]["s"] = "DOGEUSDT"
    assert connector.parse(json.dumps(frame)) == []


async def test_repair_fetches_the_missing_id_range(connector, monkeypatch):
    captured: dict = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "a": 101,
                    "p": "1.5",
                    "q": "2.0",
                    "f": 1,
                    "l": 1,
                    "T": 1_700_000_000_000,
                    "m": False,
                },
                {
                    "a": 102,
                    "p": "1.6",
                    "q": "2.1",
                    "f": 2,
                    "l": 2,
                    "T": 1_700_000_000_001,
                    "m": True,
                },
            ]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.binance.httpx.AsyncClient", lambda **kw: FakeClient())

    gap = Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    trades = await connector.repair(gap)

    assert captured["params"]["symbol"] == "BTCUSDT"
    assert captured["params"]["fromId"] == 101
    assert [t.trade_id for t in trades] == ["101", "102"]
    assert all(t.is_backfill for t in trades)
    assert all(t.source is Source.REST_REPAIR for t in trades)


async def test_repair_does_not_return_ids_at_or_beyond_the_resume_point(connector, monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {"a": 104, "p": "1", "q": "1", "T": 1, "m": False},
                {"a": 105, "p": "1", "q": "1", "T": 1, "m": False},
                {"a": 106, "p": "1", "q": "1", "T": 1, "m": False},
            ]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.binance.httpx.AsyncClient", lambda **kw: FakeClient())
    gap = Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    trades = await connector.repair(gap)
    assert [t.trade_id for t in trades] == ["104"]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/connectors/test_binance.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.connectors'`

- [ ] **Step 4: Write the connector protocol**

`ingest/connectors/__init__.py`: empty file.

`ingest/connectors/base.py`:

```python
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ingest.core.gaps import Gap
from ingest.core.models import Trade


@runtime_checkable
class Connector(Protocol):
    """One venue's wire format and nothing else.

    Resilience, rate limiting, queueing, and publishing all live in core/;
    a connector only knows how to address a venue and read its frames.
    """

    venue: str

    def stream_url(self, symbols: list[str]) -> str: ...

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]: ...

    def parse(self, raw: str) -> list[Trade]: ...

    async def repair(self, gap: Gap) -> list[Trade]: ...
```

- [ ] **Step 5: Write the Binance connector**

`ingest/connectors/binance.py`:

```python
from __future__ import annotations

import json
import time
from typing import Any

import httpx
import structlog

from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source, Trade
from ingest.core.ratelimit import bucket_for

log = structlog.get_logger(__name__)

WS_BASE = "wss://stream.binance.com:9443/stream?streams="
REST_AGG_TRADES = "https://api.binance.com/api/v3/aggTrades"
MAX_REPAIR_BATCH = 1000


class BinanceConnector:
    """Binance aggregate-trade stream.

    We subscribe to @aggTrade rather than @trade because the aggregate id `a`
    shares an id space with the keyless /api/v3/aggTrades?fromId= endpoint,
    which makes exact-range gap repair possible without an API key. The raw
    @trade stream would need /api/v3/historicalTrades, which requires one.
    """

    venue = "binance"

    def __init__(self, instruments: InstrumentMap) -> None:
        self._instruments = instruments

    def stream_url(self, symbols: list[str]) -> str:
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
        return f"{WS_BASE}{streams}"

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]:
        # Streams are named in the URL; there is no subscribe frame.
        return []

    def parse(self, raw: str) -> list[Trade]:
        frame = json.loads(raw)
        data = frame.get("data", frame)
        if data.get("e") != "aggTrade":
            return []
        return [t for t in (self._to_trade(data),) if t is not None]

    def _to_trade(
        self,
        data: dict[str, Any],
        *,
        symbol: str | None = None,
        source: Source = Source.STREAM,
    ) -> Trade | None:
        venue_symbol = symbol or data["s"]
        try:
            instrument_id = self._instruments.canonical(self.venue, venue_symbol)
        except KeyError:
            log.warning("unmapped_symbol", venue=self.venue, symbol=venue_symbol)
            return None
        return Trade(
            venue=self.venue,
            venue_symbol=venue_symbol,
            instrument_id=instrument_id,
            trade_id=str(data["a"]),
            event_ts_us=int(data["T"]) * 1000,
            ingest_ts_us=time.time_ns() // 1000,
            price=str(data["p"]),
            size=str(data["q"]),
            # m=True means the buyer was the maker, so the seller crossed the spread.
            side=Side.SELL if data["m"] else Side.BUY,
            sequence=int(data["a"]),
            is_backfill=source is not Source.STREAM,
            source=source,
        )

    async def repair(self, gap: Gap) -> list[Trade]:
        """Re-fetch the missing aggregate-trade id range over REST."""
        await bucket_for(self.venue).acquire()
        params = {
            "symbol": gap.venue_symbol,
            "fromId": gap.last_seen + 1,
            "limit": min(MAX_REPAIR_BATCH, max(gap.missing_count, 1)),
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(REST_AGG_TRADES, params=params)
            response.raise_for_status()
            rows = response.json()

        trades: list[Trade] = []
        for row in rows:
            if int(row["a"]) >= gap.next_seen:
                continue  # the stream already delivered these
            trade = self._to_trade(
                row, symbol=gap.venue_symbol, source=Source.REST_REPAIR
            )
            if trade is not None:
                trades.append(trade)
        log.info(
            "gap_repaired",
            venue=self.venue,
            symbol=gap.venue_symbol,
            requested=gap.missing_count,
            recovered=len(trades),
        )
        return trades
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/connectors/test_binance.py -v`
Expected: 10 passed

- [ ] **Step 7: Commit**

```bash
git add ingest/connectors tests/connectors tests/fixtures
git commit -m "feat: Binance aggTrade connector with keyless REST gap repair"
```

---

### Task 10: Coinbase connector

**Files:**
- Create: `ingest/connectors/coinbase.py`, `tests/fixtures/coinbase_market_trades.json`
- Test: `tests/connectors/test_coinbase.py`

**Interfaces:**
- Consumes: `Connector` protocol (Task 9), `InstrumentMap`, `Trade`, `Gap`
- Produces: `class CoinbaseConnector(Connector)` with `venue = "coinbase"`

Coinbase's `sequence_num` is an envelope-level counter for the whole
connection, not per symbol, so it is tracked under the sentinel symbol `*`.
Repair is best-effort: the public market-data API exposes recent trades but no
id-range query, so we refetch the last N and let natural-key dedupe absorb the
overlap. This asymmetry with Binance is real and is documented rather than
hidden.

- [ ] **Step 1: Record the fixture**

`tests/fixtures/coinbase_market_trades.json`:

```json
{
  "channel": "market_trades",
  "client_id": "",
  "timestamp": "2026-08-07T12:00:00.000000Z",
  "sequence_num": 42,
  "events": [
    {
      "type": "update",
      "trades": [
        {
          "trade_id": "555000111",
          "product_id": "BTC-USD",
          "price": "43215.10",
          "size": "0.00250000",
          "side": "BUY",
          "time": "2026-08-07T12:00:00.123456Z"
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

`tests/connectors/test_coinbase.py`:

```python
import json
from pathlib import Path

import pytest

from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source

FIXTURE = Path(__file__).parent.parent / "fixtures" / "coinbase_market_trades.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {coinbase: BTC-USD}
"""


@pytest.fixture
def connector(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    return CoinbaseConnector(InstrumentMap.from_yaml(path))


def test_stream_url_is_the_advanced_trade_endpoint(connector):
    assert connector.stream_url(["BTC-USD"]) == "wss://advanced-trade-ws.coinbase.com"


def test_subscribe_payload_requests_market_trades(connector):
    (payload,) = connector.subscribe_payloads(["BTC-USD", "ETH-USD"])
    assert payload == {
        "type": "subscribe",
        "channel": "market_trades",
        "product_ids": ["BTC-USD", "ETH-USD"],
    }


def test_parses_the_recorded_frame(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.venue == "coinbase"
    assert trade.venue_symbol == "BTC-USD"
    assert trade.instrument_id == "BTC-USD"
    assert trade.trade_id == "555000111"
    assert trade.price == "43215.10"
    assert trade.side is Side.BUY
    assert trade.source is Source.STREAM


def test_rfc3339_time_becomes_microseconds(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.event_ts_us == 1_786_449_600_123_456


def test_envelope_sequence_is_attached_to_every_trade(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.sequence == 42


def test_snapshot_frames_are_parsed_like_updates(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["events"][0]["type"] = "snapshot"
    assert len(connector.parse(json.dumps(frame))) == 1


def test_heartbeat_and_subscription_acks_are_ignored(connector):
    assert connector.parse(json.dumps({"channel": "subscriptions", "events": []})) == []
    assert connector.parse(json.dumps({"channel": "heartbeats", "events": []})) == []


def test_unmapped_product_is_skipped(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["events"][0]["trades"][0]["product_id"] = "DOGE-USD"
    assert connector.parse(json.dumps(frame)) == []


def test_sequence_key_is_connection_wide_not_per_symbol(connector):
    assert connector.sequence_symbol == "*"


async def test_repair_refetches_recent_trades_best_effort(connector, monkeypatch):
    captured: dict = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "trades": [
                    {
                        "trade_id": "555000112",
                        "product_id": "BTC-USD",
                        "price": "43216.00",
                        "size": "0.001",
                        "side": "SELL",
                        "time": "2026-08-07T12:00:01.000000Z",
                    }
                ]
            }

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.coinbase.httpx.AsyncClient", lambda **kw: FakeClient())

    gap = Gap(venue="coinbase", venue_symbol="BTC-USD", last_seen=42, next_seen=45)
    trades = await connector.repair(gap)

    assert "BTC-USD" in captured["url"]
    assert [t.trade_id for t in trades] == ["555000112"]
    assert all(t.is_backfill for t in trades)
    assert all(t.source is Source.REST_REPAIR for t in trades)


async def test_repair_of_the_wildcard_symbol_covers_every_configured_product(
    connector, monkeypatch
):
    calls: list[str] = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"trades": []}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.coinbase.httpx.AsyncClient", lambda **kw: FakeClient())
    gap = Gap(venue="coinbase", venue_symbol="*", last_seen=42, next_seen=45)
    await connector.repair(gap)
    assert len(calls) == 1  # one product configured in this fixture universe
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/connectors/test_coinbase.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.connectors.coinbase'`

- [ ] **Step 4: Write the connector**

`ingest/connectors/coinbase.py`:

```python
from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import httpx
import structlog

from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source, Trade
from ingest.core.ratelimit import bucket_for

log = structlog.get_logger(__name__)

WS_URL = "wss://advanced-trade-ws.coinbase.com"
REST_TICKER = "https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/ticker"
REPAIR_LIMIT = 100


class CoinbaseConnector:
    """Coinbase Advanced Trade market_trades channel.

    Two things differ from Binance and both are load-bearing:

    * `sequence_num` is a connection-wide counter, not per product, so gaps are
      tracked under the sentinel symbol "*".
    * The public market-data API has no id-range query, so repair is
      best-effort: refetch the most recent trades per product and let
      natural-key dedupe in Silver absorb the overlap. Not every venue supports
      the same repair, and pretending otherwise would hide a real limitation.
    """

    venue = "coinbase"
    sequence_symbol = "*"

    def __init__(self, instruments: InstrumentMap) -> None:
        self._instruments = instruments

    def stream_url(self, symbols: list[str]) -> str:
        return WS_URL

    def subscribe_payloads(self, symbols: list[str]) -> list[dict[str, Any]]:
        return [{"type": "subscribe", "channel": "market_trades", "product_ids": symbols}]

    def parse(self, raw: str) -> list[Trade]:
        frame = json.loads(raw)
        if frame.get("channel") != "market_trades":
            return []
        sequence = frame.get("sequence_num")
        trades: list[Trade] = []
        for event in frame.get("events", []):
            for row in event.get("trades", []):
                trade = self._to_trade(row, sequence=sequence, source=Source.STREAM)
                if trade is not None:
                    trades.append(trade)
        return trades

    def _to_trade(
        self, row: dict[str, Any], *, sequence: int | None, source: Source
    ) -> Trade | None:
        venue_symbol = row["product_id"]
        try:
            instrument_id = self._instruments.canonical(self.venue, venue_symbol)
        except KeyError:
            log.warning("unmapped_symbol", venue=self.venue, symbol=venue_symbol)
            return None
        return Trade(
            venue=self.venue,
            venue_symbol=venue_symbol,
            instrument_id=instrument_id,
            trade_id=str(row["trade_id"]),
            event_ts_us=_rfc3339_to_us(row["time"]),
            ingest_ts_us=time.time_ns() // 1000,
            price=str(row["price"]),
            size=str(row["size"]),
            side=Side(row["side"]) if row["side"] in ("BUY", "SELL") else Side.UNKNOWN,
            sequence=int(sequence) if sequence is not None else None,
            is_backfill=source is not Source.STREAM,
            source=source,
        )

    async def repair(self, gap: Gap) -> list[Trade]:
        products = (
            self._instruments.symbols_for(self.venue)
            if gap.venue_symbol == self.sequence_symbol
            else [gap.venue_symbol]
        )
        recovered: list[Trade] = []
        async with httpx.AsyncClient(timeout=10.0) as client:
            for product_id in products:
                await bucket_for(self.venue).acquire()
                response = await client.get(
                    REST_TICKER.format(product_id=product_id), params={"limit": REPAIR_LIMIT}
                )
                response.raise_for_status()
                for row in response.json().get("trades", []):
                    row.setdefault("product_id", product_id)
                    trade = self._to_trade(row, sequence=None, source=Source.REST_REPAIR)
                    if trade is not None:
                        recovered.append(trade)
        log.info("gap_repaired", venue=self.venue, products=len(products), recovered=len(recovered))
        return recovered


def _rfc3339_to_us(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1_000_000)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/connectors/test_coinbase.py -v`
Expected: 11 passed

- [ ] **Step 6: Verify the Connector protocol is satisfied by both venues**

`tests/connectors/test_protocol.py`:

```python
from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.instruments import InstrumentMap


def test_both_connectors_satisfy_the_protocol(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(
        "instruments:\n"
        "  - id: BTC-USD\n"
        "    asset_class: crypto\n"
        "    venues: {binance: BTCUSDT, coinbase: BTC-USD}\n"
    )
    imap = InstrumentMap.from_yaml(path)
    assert isinstance(BinanceConnector(imap), Connector)
    assert isinstance(CoinbaseConnector(imap), Connector)
```

Run: `uv run pytest tests/connectors/test_protocol.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ingest/connectors/coinbase.py tests/connectors tests/fixtures
git commit -m "feat: Coinbase market_trades connector with best-effort repair"
```

---

### Task 11: Ingest runner wiring connectors to Kafka

**Files:**
- Create: `ingest/runner.py`, `ingest/settings.py`, `ingest/cli.py`
- Test: `tests/test_runner.py`

**Interfaces:**
- Consumes: everything from Tasks 2–10
- Produces:
  - `class Settings(BaseSettings)` with `bootstrap_servers: str`, `sasl_username: str | None`, `sasl_password: str | None`, `universe_path: Path`, `venues: list[str]`, `queue_maxsize: int`
  - `class IngestRunner: __init__(self, connector, producer, tracker, queue, topic="md.trades.v1"); async handle_message(self, raw: str) -> None; async handle_reconnect(self) -> None; async drain(self, stop: asyncio.Event) -> None; async run(self, stop: asyncio.Event) -> None`

- [ ] **Step 1: Write the failing test**

`tests/test_runner.py`:

```python
import asyncio
import json
from pathlib import Path

import pytest

from ingest.connectors.binance import BinanceConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Source
from ingest.core.queue import BoundedTopicQueue, DropPolicy
from ingest.runner import IngestRunner

FIXTURE = Path(__file__).parent / "fixtures" / "binance_aggtrade.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT}
"""


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    def produce(self, topic, trade):
        self.sent.append((topic, trade))

    def poll(self, timeout=0.0):
        return 0

    def flush(self, timeout=10.0):
        return 0


@pytest.fixture
def wiring(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    connector = BinanceConnector(InstrumentMap.from_yaml(path))
    producer = RecordingProducer()
    queue = BoundedTopicQueue(maxsize=64, policy=DropPolicy.BLOCK)
    runner = IngestRunner(connector, producer, SequenceTracker(), queue)
    return runner, producer, queue


async def test_message_lands_on_the_queue(wiring):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())
    assert queue.qsize() == 1


async def test_drain_publishes_queued_trades(wiring):
    runner, producer, _ = wiring
    await runner.handle_message(FIXTURE.read_text())
    stop = asyncio.Event()
    task = asyncio.create_task(runner.drain(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert producer.sent[0][0] == "md.trades.v1"
    assert producer.sent[0][1].trade_id == "987654321"


async def test_a_sequence_jump_triggers_repair_and_enqueues_the_recovered_trades(
    wiring, monkeypatch
):
    runner, _, queue = wiring
    first = json.loads(FIXTURE.read_text())
    await runner.handle_message(json.dumps(first))

    async def fake_repair(gap):
        from ingest.core.models import Side, Trade

        return [
            Trade(
                venue="binance",
                venue_symbol="BTCUSDT",
                instrument_id="BTC-USD",
                trade_id=str(gap.last_seen + 1),
                event_ts_us=1,
                ingest_ts_us=1,
                price="1",
                size="1",
                side=Side.BUY,
                sequence=gap.last_seen + 1,
                is_backfill=True,
                source=Source.REST_REPAIR,
            )
        ]

    monkeypatch.setattr(runner.connector, "repair", fake_repair)

    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 987654325
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)

    assert queue.qsize() == 3  # original + repaired + jumped


async def test_repair_failure_does_not_kill_the_stream(wiring, monkeypatch, caplog):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())

    async def failing_repair(gap):
        raise RuntimeError("venue is down")

    monkeypatch.setattr(runner.connector, "repair", failing_repair)

    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 987654325
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)

    assert queue.qsize() == 2  # the live trades still made it through


async def test_reconnect_resets_the_watermark_so_a_new_stream_is_not_a_gap(wiring):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())
    await runner.handle_reconnect()
    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 999999999
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)
    assert queue.qsize() == 2  # no repair records injected
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingest.runner'`

- [ ] **Step 3: Write settings**

`ingest/settings.py`:

```python
from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", env_file=".env")

    bootstrap_servers: str = "localhost:9092"
    sasl_username: str | None = None
    sasl_password: str | None = None
    universe_path: Path = Path("config/universe.yaml")
    venues: list[str] = ["binance", "coinbase"]
    queue_maxsize: int = 20_000
    trades_topic: str = "md.trades.v1"
```

- [ ] **Step 4: Write the runner**

`ingest/runner.py`:

```python
from __future__ import annotations

import asyncio
import contextlib
from typing import Protocol

import structlog

from ingest.connectors.base import Connector
from ingest.core.gaps import SequenceTracker
from ingest.core.models import Trade
from ingest.core.queue import BoundedTopicQueue
from ingest.core.ws import ResilientWebSocket

log = structlog.get_logger(__name__)


class ProducerLike(Protocol):
    def produce(self, topic: str, trade: Trade) -> None: ...
    def poll(self, timeout: float = 0.0) -> int: ...
    def flush(self, timeout: float = 10.0) -> int: ...


class IngestRunner:
    """Glues one connector to one Kafka topic.

    Parsing populates a bounded queue; a separate drain task publishes. The
    split matters: publishing must never block frame consumption, and the
    queue's policy (BLOCK for trades) is what turns saturation into a
    detectable, repairable gap rather than silent loss.
    """

    def __init__(
        self,
        connector: Connector,
        producer: ProducerLike,
        tracker: SequenceTracker,
        queue: BoundedTopicQueue[Trade],
        topic: str = "md.trades.v1",
    ) -> None:
        self.connector = connector
        self.producer = producer
        self.tracker = tracker
        self.queue = queue
        self.topic = topic
        self._repair_tasks: set[asyncio.Task[None]] = set()

    async def handle_message(self, raw: str) -> None:
        for trade in self.connector.parse(raw):
            sequence_symbol = getattr(self.connector, "sequence_symbol", trade.venue_symbol)
            gap = self.tracker.observe(trade.venue, sequence_symbol, trade.sequence)
            if gap is not None:
                task = asyncio.create_task(self._repair(gap))
                self._repair_tasks.add(task)
                task.add_done_callback(self._repair_tasks.discard)
            await self.queue.put(trade)

    async def _repair(self, gap: object) -> None:
        try:
            for trade in await self.connector.repair(gap):  # type: ignore[arg-type]
                await self.queue.put(trade)
        except Exception as exc:  # noqa: BLE001 — a failed repair must not stop the stream
            log.error("gap_repair_failed", error=str(exc))

    async def handle_reconnect(self) -> None:
        """A fresh connection restarts the venue's sequence; forget the watermark."""
        self.tracker = SequenceTracker()
        log.info("sequence_watermarks_reset", venue=self.connector.venue)

    async def drain(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                trade = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                self.producer.poll(0)
                continue
            self.producer.produce(self.topic, trade)
            self.producer.poll(0)
        self.producer.flush(10.0)

    async def run(self, stop: asyncio.Event, symbols: list[str]) -> None:
        socket = ResilientWebSocket(
            self.connector.stream_url(symbols),
            self.connector.subscribe_payloads(symbols),
            self.handle_message,
            self.handle_reconnect,
        )
        drain_task = asyncio.create_task(self.drain(stop))
        try:
            await socket.run(stop)
        finally:
            stop.set()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_runner.py -v`
Expected: 5 passed

- [ ] **Step 6: Write the CLI**

`ingest/cli.py`:

```python
from __future__ import annotations

import asyncio
import signal
import sys

import structlog

from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.producer import TradeProducer
from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue
from ingest.runner import IngestRunner
from ingest.settings import Settings

log = structlog.get_logger(__name__)

CONNECTORS = {"binance": BinanceConnector, "coinbase": CoinbaseConnector}


def build_connector(venue: str, instruments: InstrumentMap) -> Connector:
    try:
        return CONNECTORS[venue](instruments)
    except KeyError:
        raise SystemExit(f"unknown venue {venue!r}; known: {sorted(CONNECTORS)}") from None


async def main() -> None:
    structlog.configure(processors=[structlog.processors.JSONRenderer()])
    settings = Settings()
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
            TradeProducer(
                settings.bootstrap_servers,
                sasl_username=settings.sasl_username,
                sasl_password=settings.sasl_password,
            ),
            SequenceTracker(),
            BoundedTopicQueue(settings.queue_maxsize, TOPIC_POLICIES[settings.trades_topic]),
            settings.trades_topic,
        )
        log.info("starting_venue", venue=venue, symbols=len(symbols))
        tasks.append(asyncio.create_task(runner.run(stop, symbols)))

    if not tasks:
        raise SystemExit("no venues to run")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))  # type: ignore[func-returns-value]
```

- [ ] **Step 7: Verify the CLI wires up without connecting**

Run: `INGEST_VENUES='["binance"]' uv run python -c "from ingest.cli import build_connector; from ingest.core.instruments import InstrumentMap; from pathlib import Path; c = build_connector('binance', InstrumentMap.from_yaml(Path('config/universe.yaml'))); print(c.stream_url(['BTCUSDT']))"`
Expected: prints the combined-stream URL containing `btcusdt@aggTrade`

- [ ] **Step 8: Run the full suite and commit**

```bash
make check
git add ingest/runner.py ingest/settings.py ingest/cli.py tests/test_runner.py
git commit -m "feat: ingest runner wiring connectors, gap repair, and Kafka publishing"
```

---

### Task 12: Local docker-compose stack and end-to-end integration test

**Files:**
- Create: `docker/Dockerfile`, `docker/compose.yaml`, `tests/integration/test_end_to_end.py`, `tests/integration/conftest.py`
- Modify: `Makefile` (add `compose-up`, `compose-down`, `test-integration`)
- Modify: `.github/workflows/ci.yml` (add an integration job)

**Interfaces:**
- Consumes: `ingest.cli`, `TradeProducer`, `trade_codec`
- Produces: `make compose-up` bringing up single-broker Kafka on `localhost:9092`; `make test-integration`

- [ ] **Step 1: Write the failing integration test**

`tests/integration/conftest.py`:

```python
import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set RUN_INTEGRATION=1 with docker compose running")
    for item in items:
        item.add_marker(skip)
```

`tests/integration/test_end_to_end.py`:

```python
"""Round-trips real records through a real broker.

Guarded by RUN_INTEGRATION=1 so `make test` stays fast and hermetic.
"""

import uuid

import pytest
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade
from ingest.core.producer import TradeProducer

BOOTSTRAP = "localhost:9092"


@pytest.fixture
def topic() -> str:
    name = f"test.trades.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    future = admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1)])[name]
    future.result(timeout=15)
    yield name
    admin.delete_topics([name])


def make_trade(trade_id: str, symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        venue="binance",
        venue_symbol=symbol,
        instrument_id="BTC-USD",
        trade_id=trade_id,
        event_ts_us=1_700_000_000_000_000,
        ingest_ts_us=1_700_000_000_500_000,
        price="43210.55",
        size="0.0012",
        side=Side.BUY,
        sequence=int(trade_id),
        is_backfill=False,
        source=Source.STREAM,
    )


def consume(topic: str, expected: int, timeout_s: float = 20.0):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages = []
    import time

    deadline = time.monotonic() + timeout_s
    while len(messages) < expected and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        messages.append(msg)
    consumer.close()
    return messages


def test_produced_trades_are_consumable_and_decodable(topic):
    producer = TradeProducer(BOOTSTRAP)
    for i in range(10):
        producer.produce(topic, make_trade(str(100 + i)))
    assert producer.flush(30.0) == 0

    messages = consume(topic, expected=10)
    assert len(messages) == 10

    codec = trade_codec()
    decoded = [codec.decode(m.value()) for m in messages]
    assert {d["trade_id"] for d in decoded} == {str(100 + i) for i in range(10)}


def test_schema_version_header_survives_the_broker(topic):
    producer = TradeProducer(BOOTSTRAP)
    producer.produce(topic, make_trade("500"))
    producer.flush(30.0)

    (message,) = consume(topic, expected=1)
    headers = dict(message.headers())
    assert headers["schema_version"] == str(TRADE_SCHEMA_VERSION).encode()


def test_same_symbol_always_lands_on_one_partition(topic):
    producer = TradeProducer(BOOTSTRAP)
    for i in range(20):
        producer.produce(topic, make_trade(str(600 + i)))
    producer.flush(30.0)

    messages = consume(topic, expected=20)
    assert len({m.partition() for m in messages}) == 1, "ordering per instrument requires one partition"


def test_different_symbols_can_spread_across_partitions(topic):
    producer = TradeProducer(BOOTSTRAP)
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"):
        for i in range(5):
            producer.produce(topic, make_trade(str(700 + i), symbol=symbol))
    producer.flush(30.0)

    messages = consume(topic, expected=20)
    assert len({m.partition() for m in messages}) >= 2
```

- [ ] **Step 2: Run test to verify it is skipped without the stack**

Run: `uv run pytest tests/integration -v`
Expected: 4 skipped

- [ ] **Step 3: Write the compose stack**

`docker/compose.yaml`:

```yaml
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: fdai-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 5s
      timeout: 10s
      retries: 20

  producers:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    container_name: fdai-producers
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      INGEST_BOOTSTRAP_SERVERS: kafka:9092
      INGEST_UNIVERSE_PATH: /app/config/universe.yaml
    profiles: ["live"]
    restart: unless-stopped
```

`docker/Dockerfile`:

```dockerfile
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY ingest/ ./ingest/
COPY config/ ./config/

ENV PATH="/app/.venv/bin:${PATH}"
CMD ["python", "-m", "ingest.cli"]
```

Note the `producers` service sits behind the `live` profile so `make compose-up`
starts only Kafka — integration tests must not depend on a live exchange feed.

- [ ] **Step 4: Add Makefile targets**

Append to `Makefile`:

```makefile
.PHONY: compose-up compose-down test-integration

compose-up:
	docker compose -f docker/compose.yaml up -d --wait kafka

compose-down:
	docker compose -f docker/compose.yaml down -v

test-integration: compose-up
	RUN_INTEGRATION=1 uv run pytest tests/integration -v
```

- [ ] **Step 5: Run the integration tests against the real broker**

Run: `make test-integration`
Expected: 4 passed

- [ ] **Step 6: Verify the live producer path against real exchanges**

Run: `docker compose -f docker/compose.yaml --profile live up -d --build`
Then: `docker compose -f docker/compose.yaml logs -f producers`
Expected: JSON log lines with `starting_venue`, then no errors for 60 seconds.
Then: `docker exec fdai-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic md.trades.v1 --from-beginning --max-messages 5`

Note: the topic must exist first — create it with
`docker exec fdai-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic md.trades.v1 --partitions 6 --replication-factor 1`

Expected: five binary Avro records scroll past. Then `docker compose -f docker/compose.yaml --profile live down`.

- [ ] **Step 7: Add the CI integration job**

Append to `.github/workflows/ci.yml` under `jobs:`:

```yaml
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - run: docker compose -f docker/compose.yaml up -d --wait kafka
      - run: RUN_INTEGRATION=1 uv run pytest tests/integration -v
      - if: always()
        run: docker compose -f docker/compose.yaml down -v
```

- [ ] **Step 8: Commit**

```bash
git add docker tests/integration Makefile .github/workflows/ci.yml
git commit -m "feat: local Kafka stack and end-to-end integration tests"
```

---

### Task 13: Terraform state backend bootstrap

**Files:**
- Create: `infra/bootstrap/main.tf`, `infra/bootstrap/variables.tf`, `infra/bootstrap/outputs.tf`, `infra/bootstrap/README.md`
- Test: `infra/bootstrap/.terraform.lock.hcl` via `terraform validate`

**Interfaces:**
- Consumes: nothing
- Produces: an S3 bucket named `${var.project}-tfstate-${data.aws_caller_identity.current.account_id}` and a DynamoDB lock table `${var.project}-tflock`, consumed as the backend by `infra/envs/dev`.

Chicken-and-egg: the state backend cannot itself be stored in the backend, so
this layer uses local state and is disposable along with the account.

- [ ] **Step 1: Write the bootstrap layer**

`infra/bootstrap/variables.tf`:

```hcl
variable "project" {
  description = "Prefix for all resource names."
  type        = string
  default     = "fdai"
}

variable "region" {
  description = "AWS region for the sandbox stack."
  type        = string
  default     = "us-east-1"
}
```

`infra/bootstrap/main.tf`:

```hcl
terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Local state on purpose: this layer creates the remote backend, and it is
  # destroyed together with the account it lives in.
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

resource "aws_s3_bucket" "state" {
  bucket        = "${var.project}-tfstate-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "lock" {
  name         = "${var.project}-tflock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}
```

`infra/bootstrap/outputs.tf`:

```hcl
output "state_bucket" {
  value = aws_s3_bucket.state.id
}

output "lock_table" {
  value = aws_dynamodb_table.lock.name
}

output "region" {
  value = var.region
}
```

`infra/bootstrap/README.md`:

```markdown
# Terraform state bootstrap

Creates the S3 bucket and DynamoDB lock table that `infra/envs/dev` uses as its
backend. Uses **local state**, because a backend cannot store its own state.

Losing this state is normally a disaster. Here it is not: the sandbox account is
wiped every 7 days, so the state and the resources it describes disappear
together and stay consistent (both empty). `make up` re-runs this layer from
scratch each cycle.

Run directly only for debugging — `make up` invokes it.
```

- [ ] **Step 2: Validate**

Run: `terraform -chdir=infra/bootstrap init -backend=false && terraform -chdir=infra/bootstrap validate && terraform fmt -check -recursive infra/`
Expected: `Success! The configuration is valid.` and no fmt diffs.

- [ ] **Step 3: Verify no IAM resources leaked in**

Run: `! grep -rE 'resource +"aws_iam_(role|role_policy|instance_profile)' infra/`
Expected: exit 0 (no matches — the global constraint holds)

- [ ] **Step 4: Commit**

```bash
git add infra/bootstrap
git commit -m "feat(infra): disposable Terraform state backend bootstrap"
```

---

### Task 14: Network module

**Files:**
- Create: `infra/modules/network/main.tf`, `infra/modules/network/variables.tf`, `infra/modules/network/outputs.tf`

**Interfaces:**
- Consumes: `var.project`, `var.region`
- Produces outputs: `vpc_id`, `public_subnet_ids` (list, one per AZ), `msk_security_group_id`, `producer_security_group_id`

- [ ] **Step 1: Write the module**

`infra/modules/network/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "az_count" {
  description = "MSK requires at least two AZs."
  type        = number
  default     = 2
}

variable "kafka_client_cidrs" {
  description = "CIDRs allowed to reach the brokers — the Databricks workspace NAT EIP, plus your own IP."
  type        = list(string)
}
```

`infra/modules/network/main.tf`:

```hcl
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.project}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.project}-igw" }
}

resource "aws_subnet" "public" {
  count                   = var.az_count
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.project}-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.project}-public" }
}

resource "aws_route_table_association" "public" {
  count          = var.az_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "msk" {
  name        = "${var.project}-msk"
  description = "Kafka brokers: SASL_SSL from allowlisted clients only"
  vpc_id      = aws_vpc.this.id

  # 9196 is the public SASL/SCRAM port; 9096 is the in-VPC SASL/SCRAM port.
  ingress {
    description = "Kafka SASL_SSL public"
    from_port   = 9196
    to_port     = 9196
    protocol    = "tcp"
    cidr_blocks = var.kafka_client_cidrs
  }

  ingress {
    description = "Kafka SASL_SSL in-VPC"
    from_port   = 9096
    to_port     = 9096
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-msk" }
}

resource "aws_security_group" "producer" {
  name        = "${var.project}-producer"
  description = "Producer host: egress only"
  vpc_id      = aws_vpc.this.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-producer" }
}
```

`infra/modules/network/outputs.tf`:

```hcl
output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}

output "producer_security_group_id" {
  value = aws_security_group.producer.id
}
```

- [ ] **Step 2: Validate**

Run: `terraform -chdir=infra/modules/network init -backend=false && terraform -chdir=infra/modules/network validate`
Expected: valid

- [ ] **Step 3: Commit**

```bash
git add infra/modules/network
git commit -m "feat(infra): VPC, public subnets, and Kafka security groups"
```

---

### Task 15: MSK module with SASL/SCRAM

**Files:**
- Create: `infra/modules/kafka_msk/main.tf`, `infra/modules/kafka_msk/variables.tf`, `infra/modules/kafka_msk/outputs.tf`

**Interfaces:**
- Consumes: `vpc_id`, `public_subnet_ids`, `msk_security_group_id` from Task 14
- Produces outputs: `bootstrap_brokers_sasl_scram_public`, `bootstrap_brokers_sasl_scram`, `sasl_username`, `sasl_password` (sensitive), `secret_arn`

**No IAM roles.** MSK cluster creation needs none; SASL/SCRAM auth uses a
Secrets Manager secret encrypted with a customer-managed KMS key (AWS-managed
keys are rejected by MSK for this association) plus a resource policy on the
secret. Public access is enabled in a second apply because AWS rejects it on a
cluster that is still `CREATING`.

- [ ] **Step 1: Write the module**

`infra/modules/kafka_msk/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "broker_count" {
  description = "Must be a multiple of the subnet count."
  type        = number
  default     = 2
}

variable "broker_ebs_gb" {
  description = "50 GB covers 24h retention at ~5 GB/day with headroom."
  type        = number
  default     = 50
}

variable "public_access" {
  description = "Enable on the second apply; AWS rejects it while the cluster is CREATING."
  type        = bool
  default     = false
}
```

`infra/modules/kafka_msk/main.tf`:

```hcl
resource "random_password" "sasl" {
  length  = 32
  special = false
}

resource "aws_kms_key" "secrets" {
  description             = "${var.project} MSK SCRAM secret encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.project}-msk-scram"
  target_key_id = aws_kms_key.secrets.key_id
}

# MSK requires the secret name to start with "AmazonMSK_".
resource "aws_secretsmanager_secret" "scram" {
  name                    = "AmazonMSK_${var.project}_producer"
  kms_key_id              = aws_kms_key.secrets.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "scram" {
  secret_id = aws_secretsmanager_secret.scram.id
  secret_string = jsonencode({
    username = "${var.project}-producer"
    password = random_password.sasl.result
  })
}

# A resource policy, not an IAM role — grants the MSK service read access.
resource "aws_secretsmanager_secret_policy" "scram" {
  secret_arn = aws_secretsmanager_secret.scram.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AWSKafkaResourcePolicy"
      Effect    = "Allow"
      Principal = { Service = "kafka.amazonaws.com" }
      Action    = "secretsmanager:getSecretValue"
      Resource  = aws_secretsmanager_secret.scram.arn
    }]
  })
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.project}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_gb
      }
    }

    dynamic "connectivity_info" {
      for_each = var.public_access ? [1] : []
      content {
        public_access {
          type = "SERVICE_PROVIDED_EIPS"
        }
      }
    }
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

resource "aws_msk_scram_secret_association" "this" {
  cluster_arn     = aws_msk_cluster.this.arn
  secret_arn_list = [aws_secretsmanager_secret.scram.arn]

  depends_on = [aws_secretsmanager_secret_version.scram]
}
```

`infra/modules/kafka_msk/outputs.tf`:

```hcl
output "bootstrap_brokers_sasl_scram" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_scram
}

output "bootstrap_brokers_sasl_scram_public" {
  value = aws_msk_cluster.this.bootstrap_brokers_public_sasl_scram
}

output "sasl_username" {
  value = "${var.project}-producer"
}

output "sasl_password" {
  value     = random_password.sasl.result
  sensitive = true
}

output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}
```

- [ ] **Step 2: Validate and re-check the IAM constraint**

Run: `terraform -chdir=infra/modules/kafka_msk init -backend=false && terraform -chdir=infra/modules/kafka_msk validate`
Expected: valid

Run: `! grep -rE 'resource +"aws_iam_(role|role_policy|instance_profile)' infra/`
Expected: exit 0

- [ ] **Step 3: Commit**

```bash
git add infra/modules/kafka_msk
git commit -m "feat(infra): MSK module with SASL/SCRAM auth and no IAM roles"
```

---

### Task 16: Producer host module

**Files:**
- Create: `infra/modules/producer_host/main.tf`, `infra/modules/producer_host/variables.tf`, `infra/modules/producer_host/outputs.tf`, `infra/modules/producer_host/user_data.sh.tftpl`

**Interfaces:**
- Consumes: `subnet_id`, `security_group_id` from Task 14; `bootstrap_brokers_sasl_scram`, `sasl_username`, `sasl_password` from Task 15
- Produces outputs: `instance_id`, `public_ip`

The host clones the repo and builds the image locally. That deliberately avoids
ECR, which would require an instance profile to pull from — an IAM role we
cannot create.

- [ ] **Step 1: Write the module**

`infra/modules/producer_host/variables.tf`:

```hcl
variable "project" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "security_group_id" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "repo_url" {
  description = "Public HTTPS clone URL for this repository."
  type        = string
}

variable "repo_ref" {
  type    = string
  default = "main"
}

variable "bootstrap_servers" {
  type = string
}

variable "sasl_username" {
  type = string
}

variable "sasl_password" {
  type      = string
  sensitive = true
}

variable "venues" {
  type    = list(string)
  default = ["binance", "coinbase"]
}

variable "instance_profile_name" {
  description = "Pre-existing instance profile, if the account provides one. Null means none."
  type        = string
  default     = null
}
```

`infra/modules/producer_host/user_data.sh.tftpl`:

```bash
#!/bin/bash
set -euxo pipefail

dnf install -y docker git
systemctl enable --now docker

mkdir -p /opt/fdai
cd /opt/fdai
git clone --depth 1 --branch ${repo_ref} ${repo_url} app
cd app

cat > .env <<'ENVEOF'
INGEST_BOOTSTRAP_SERVERS=${bootstrap_servers}
INGEST_SASL_USERNAME=${sasl_username}
INGEST_SASL_PASSWORD=${sasl_password}
INGEST_UNIVERSE_PATH=/app/config/universe.yaml
INGEST_VENUES=${venues_json}
ENVEOF
chmod 600 .env

docker build -f docker/Dockerfile -t fdai-producers .
docker run -d --name fdai-producers --restart unless-stopped --env-file .env fdai-producers
```

`infra/modules/producer_host/main.tf`:

```hcl
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_instance" "producer" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  associate_public_ip_address = true

  # Null unless the account provides a pre-existing profile; we never create one.
  iam_instance_profile = var.instance_profile_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    bootstrap_servers = var.bootstrap_servers
    sasl_username     = var.sasl_username
    sasl_password     = var.sasl_password
    venues_json       = jsonencode(var.venues)
  })

  user_data_replace_on_change = true

  root_block_device {
    volume_size = 20
    encrypted   = true
  }

  tags = { Name = "${var.project}-producer" }
}
```

`infra/modules/producer_host/outputs.tf`:

```hcl
output "instance_id" {
  value = aws_instance.producer.id
}

output "public_ip" {
  value = aws_instance.producer.public_ip
}
```

- [ ] **Step 2: Validate**

Run: `terraform -chdir=infra/modules/producer_host init -backend=false && terraform -chdir=infra/modules/producer_host validate`
Expected: valid

- [ ] **Step 3: Commit**

```bash
git add infra/modules/producer_host
git commit -m "feat(infra): producer host that builds locally to avoid ECR and IAM"
```

---

### Task 17: Dev environment composition

**Files:**
- Create: `infra/envs/dev/main.tf`, `infra/envs/dev/variables.tf`, `infra/envs/dev/outputs.tf`, `infra/envs/dev/backend.tf.tftpl`, `infra/envs/dev/terraform.tfvars.example`

**Interfaces:**
- Consumes: all three modules
- Produces outputs consumed by `scripts/bootstrap.sh` (Task 18): `bootstrap_brokers_public`, `sasl_username`, `sasl_password` (sensitive), `producer_public_ip`

- [ ] **Step 1: Write the environment**

`infra/envs/dev/variables.tf`:

```hcl
variable "project" {
  type    = string
  default = "fdai"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "kafka_client_cidrs" {
  description = "Databricks workspace NAT EIP as /32, plus your own IP as /32."
  type        = list(string)
}

variable "repo_url" {
  type = string
}

variable "repo_ref" {
  type    = string
  default = "main"
}

variable "msk_public_access" {
  description = "False on the first apply, true on the second. make up handles both."
  type        = bool
  default     = false
}

variable "instance_profile_name" {
  description = "Pre-existing instance profile if the account has one; null otherwise."
  type        = string
  default     = null
}
```

`infra/envs/dev/main.tf`:

```hcl
terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
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

module "network" {
  source             = "../../modules/network"
  project            = var.project
  kafka_client_cidrs = var.kafka_client_cidrs
}

module "kafka" {
  source            = "../../modules/kafka_msk"
  project           = var.project
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.msk_security_group_id
  public_access     = var.msk_public_access
}

module "producer_host" {
  source                = "../../modules/producer_host"
  project               = var.project
  subnet_id             = module.network.public_subnet_ids[0]
  security_group_id     = module.network.producer_security_group_id
  repo_url              = var.repo_url
  repo_ref              = var.repo_ref
  bootstrap_servers     = module.kafka.bootstrap_brokers_sasl_scram
  sasl_username         = module.kafka.sasl_username
  sasl_password         = module.kafka.sasl_password
  instance_profile_name = var.instance_profile_name
}
```

`infra/envs/dev/outputs.tf`:

```hcl
output "bootstrap_brokers_public" {
  value = module.kafka.bootstrap_brokers_sasl_scram_public
}

output "bootstrap_brokers_private" {
  value = module.kafka.bootstrap_brokers_sasl_scram
}

output "sasl_username" {
  value = module.kafka.sasl_username
}

output "sasl_password" {
  value     = module.kafka.sasl_password
  sensitive = true
}

output "producer_public_ip" {
  value = module.producer_host.public_ip
}
```

`infra/envs/dev/backend.tf.tftpl`:

```hcl
terraform {
  backend "s3" {
    bucket         = "${state_bucket}"
    key            = "dev/terraform.tfstate"
    region         = "${region}"
    dynamodb_table = "${lock_table}"
    encrypt        = true
  }
}
```

`infra/envs/dev/terraform.tfvars.example`:

```hcl
# Copy to terraform.tfvars and fill in. terraform.tfvars is gitignored.
region             = "us-east-1"
repo_url           = "https://github.com/YOUR_USER/finance_data_ai.git"
kafka_client_cidrs = ["203.0.113.10/32", "198.51.100.25/32"] # databricks NAT EIP, your IP
```

- [ ] **Step 2: Add `terraform.tfvars` to gitignore**

Append to `.gitignore`:

```text
infra/**/terraform.tfvars
infra/**/backend.tf
infra/**/.terraform.lock.hcl
```

- [ ] **Step 3: Validate**

Run: `terraform -chdir=infra/envs/dev init -backend=false && terraform -chdir=infra/envs/dev validate && terraform fmt -check -recursive infra/`
Expected: valid, no fmt diffs

- [ ] **Step 4: Commit**

```bash
git add infra/envs .gitignore
git commit -m "feat(infra): dev environment composing network, MSK, and producer host"
```

---

### Task 18: Bootstrap script — topics and Databricks secret scope

**Files:**
- Create: `scripts/bootstrap.sh`, `scripts/create_topics.py`, `scripts/smoke_test.py`
- Test: `tests/test_create_topics.py`

**Interfaces:**
- Consumes: Terraform outputs from Task 17
- Produces: topics from `TOPIC_SPECS`; a Databricks secret scope named `fdai` holding `kafka_bootstrap`, `kafka_username`, `kafka_password`
- Produces: `TOPIC_SPECS: list[TopicSpec]` where `TopicSpec = namedtuple("TopicSpec", "name partitions retention_ms")`

- [ ] **Step 1: Write the failing test**

`tests/test_create_topics.py`:

```python
from scripts.create_topics import TOPIC_SPECS, to_new_topic


def test_every_spec_matches_the_design():
    by_name = {spec.name: spec for spec in TOPIC_SPECS}
    assert by_name["md.trades.v1"].partitions == 6
    assert by_name["md.trades.v1"].retention_ms == 24 * 3600 * 1000
    assert by_name["md.book.top.v1"].retention_ms == 6 * 3600 * 1000
    assert by_name["md.book.depth.v1"].retention_ms == 2 * 3600 * 1000
    assert by_name["md.bars.v1"].retention_ms == 48 * 3600 * 1000
    assert by_name["news.articles.v1"].retention_ms == 7 * 24 * 3600 * 1000


def test_every_data_topic_has_a_dead_letter_queue():
    names = {spec.name for spec in TOPIC_SPECS}
    for topic in ("md.trades.v1", "md.book.top.v1", "news.articles.v1"):
        assert f"_dlq.{topic}" in names


def test_new_topic_carries_retention_and_compression():
    spec = next(s for s in TOPIC_SPECS if s.name == "md.trades.v1")
    topic = to_new_topic(spec, replication_factor=2)
    assert topic.num_partitions == 6
    assert topic.config["retention.ms"] == str(24 * 3600 * 1000)
    assert topic.config["compression.type"] == "zstd"


def test_dlq_topics_are_single_partition():
    for spec in TOPIC_SPECS:
        if spec.name.startswith("_dlq."):
            assert spec.partitions == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_create_topics.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts'`

- [ ] **Step 3: Write the topic creator**

`scripts/__init__.py`: empty file.

`scripts/create_topics.py`:

```python
"""Create the topic set. Idempotent: existing topics are left alone."""

from __future__ import annotations

import argparse
import sys
from typing import NamedTuple

from confluent_kafka.admin import AdminClient, NewTopic

HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS


class TopicSpec(NamedTuple):
    name: str
    partitions: int
    retention_ms: int


TOPIC_SPECS: list[TopicSpec] = [
    TopicSpec("md.trades.v1", 6, 24 * HOUR_MS),
    TopicSpec("md.book.top.v1", 6, 6 * HOUR_MS),
    TopicSpec("md.book.depth.v1", 6, 2 * HOUR_MS),
    TopicSpec("md.bars.v1", 3, 48 * HOUR_MS),
    TopicSpec("news.articles.v1", 3, 7 * DAY_MS),
    TopicSpec("ops.metrics.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.trades.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.book.top.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.book.depth.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.bars.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.news.articles.v1", 1, 7 * DAY_MS),
]


def to_new_topic(spec: TopicSpec, replication_factor: int) -> NewTopic:
    return NewTopic(
        spec.name,
        num_partitions=spec.partitions,
        replication_factor=replication_factor,
        config={
            "retention.ms": str(spec.retention_ms),
            "compression.type": "zstd",
            "min.insync.replicas": str(max(1, replication_factor - 1)),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--replication-factor", type=int, default=2)
    args = parser.parse_args()

    config: dict[str, object] = {"bootstrap.servers": args.bootstrap}
    if args.username and args.password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": args.username,
            "sasl.password": args.password,
        }

    admin = AdminClient(config)
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [s for s in TOPIC_SPECS if s.name not in existing]
    if not wanted:
        print("all topics already exist")
        return 0

    futures = admin.create_topics([to_new_topic(s, args.replication_factor) for s in wanted])
    failed = 0
    for name, future in futures.items():
        try:
            future.result(timeout=60)
            print(f"created {name}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAILED {name}: {exc}", file=sys.stderr)
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_create_topics.py -v`
Expected: 4 passed

- [ ] **Step 5: Write the smoke test**

`scripts/smoke_test.py`:

```python
"""Assert that live trades are actually flowing. Exit non-zero if not."""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka import Consumer

from ingest.core.codec import trade_codec


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--topic", default="md.trades.v1")
    parser.add_argument("--min-messages", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    config: dict[str, object] = {
        "bootstrap.servers": args.bootstrap,
        "group.id": f"smoke-{int(time.time())}",
        "auto.offset.reset": "latest",
    }
    if args.username and args.password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": args.username,
            "sasl.password": args.password,
        }

    consumer = Consumer(config)
    consumer.subscribe([args.topic])
    codec = trade_codec()

    seen = 0
    deadline = time.monotonic() + args.timeout
    while seen < args.min_messages and time.monotonic() < deadline:
        msg = consumer.poll(2.0)
        if msg is None or msg.error():
            continue
        record = codec.decode(msg.value())
        print(f"{record['venue']} {record['venue_symbol']} {record['price']} {record['side']}")
        seen += 1
    consumer.close()

    if seen < args.min_messages:
        print(f"SMOKE FAIL: saw {seen}/{args.min_messages} messages", file=sys.stderr)
        return 1
    print(f"SMOKE OK: {seen} live trades decoded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Write the bootstrap script**

`scripts/bootstrap.sh`:

```bash
#!/usr/bin/env bash
# Empty AWS account -> streaming data, in one command.
#
# MSK rejects public access while the cluster is CREATING, so the cluster is
# applied twice: once private, once with public access enabled.
set -euo pipefail

PROJECT="${PROJECT:-fdai}"
REGION="${AWS_REGION:-us-east-1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV="${ROOT}/infra/envs/dev"

echo "==> 1/6 bootstrapping Terraform state backend"
terraform -chdir="${ROOT}/infra/bootstrap" init -input=false
terraform -chdir="${ROOT}/infra/bootstrap" apply -auto-approve \
  -var="project=${PROJECT}" -var="region=${REGION}"

STATE_BUCKET="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw state_bucket)"
LOCK_TABLE="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw lock_table)"

echo "==> 2/6 rendering backend config"
sed -e "s|\${state_bucket}|${STATE_BUCKET}|" \
    -e "s|\${lock_table}|${LOCK_TABLE}|" \
    -e "s|\${region}|${REGION}|" \
    "${DEV}/backend.tf.tftpl" > "${DEV}/backend.tf"

echo "==> 3/6 applying infrastructure (private)"
terraform -chdir="${DEV}" init -input=false -reconfigure
terraform -chdir="${DEV}" apply -auto-approve -var="msk_public_access=false"

echo "==> 4/6 enabling MSK public access"
terraform -chdir="${DEV}" apply -auto-approve -var="msk_public_access=true"

BOOTSTRAP_PUBLIC="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_public)"
BOOTSTRAP_PRIVATE="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_private)"
SASL_USER="$(terraform -chdir="${DEV}" output -raw sasl_username)"
SASL_PASS="$(terraform -chdir="${DEV}" output -raw sasl_password)"

echo "==> 5/6 creating topics and publishing endpoints to Databricks"
uv run python -m scripts.create_topics \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

# Broker DNS changes on every rebuild, so Databricks reads it from here rather
# than from a hardcoded value in a notebook.
databricks secrets create-scope "${PROJECT}" 2>/dev/null || true
databricks secrets put-secret "${PROJECT}" kafka_bootstrap --string-value "${BOOTSTRAP_PUBLIC}"
databricks secrets put-secret "${PROJECT}" kafka_username  --string-value "${SASL_USER}"
databricks secrets put-secret "${PROJECT}" kafka_password  --string-value "${SASL_PASS}"

echo "==> 6/6 smoke test (waiting for the producer host to build and connect)"
sleep 180
uv run python -m scripts.smoke_test \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

echo
echo "Ready."
echo "  public brokers : ${BOOTSTRAP_PUBLIC}"
echo "  in-VPC brokers : ${BOOTSTRAP_PRIVATE}"
echo "  producer host  : $(terraform -chdir="${DEV}" output -raw producer_public_ip)"
```

- [ ] **Step 7: Make it executable and shellcheck it**

Run: `chmod +x scripts/bootstrap.sh && shellcheck scripts/bootstrap.sh`
Expected: no errors (install with `brew install shellcheck` if missing)

- [ ] **Step 8: Commit**

```bash
git add scripts tests/test_create_topics.py
git commit -m "feat: bootstrap script creating topics and publishing endpoints to Databricks"
```

---

### Task 19: Makefile lifecycle targets and README

**Files:**
- Modify: `Makefile`
- Create: `README.md`

**Interfaces:**
- Consumes: `scripts/bootstrap.sh`, `infra/envs/dev`
- Produces: `make up`, `make down`, `make rebuild`, `make smoke`

- [ ] **Step 1: Add the lifecycle targets**

Append to `Makefile`:

```makefile
.PHONY: up down rebuild smoke

PROJECT ?= fdai
DEV := infra/envs/dev

up:
	./scripts/bootstrap.sh

down:
	terraform -chdir=$(DEV) destroy -auto-approve -var="msk_public_access=true" || true
	terraform -chdir=infra/bootstrap destroy -auto-approve

rebuild: down up

smoke:
	uv run python -m scripts.smoke_test \
	  --bootstrap "$$(terraform -chdir=$(DEV) output -raw bootstrap_brokers_public)" \
	  --username  "$$(terraform -chdir=$(DEV) output -raw sasl_username)" \
	  --password  "$$(terraform -chdir=$(DEV) output -raw sasl_password)"
```

- [ ] **Step 2: Write the README**

`README.md`:

```markdown
# finance_data_ai

Streaming market-data and LLM trading-decision platform.
Design: [`docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md`](docs/superpowers/specs/2026-08-07-finance-data-ai-platform-design.md)

## The reproducibility contract

The AWS sandbox account is wiped every 7 days, so **nothing durable lives in
it**. Unity Catalog managed Delta in the permanent Databricks account is the
system of record; the sandbox is pure ephemeral compute.

- `make up` — empty AWS account to streaming data. Target: under 20 minutes.
- `make down` — destroys the sandbox. Databricks is untouched.
- `make rebuild` — both, in order.
- **Any manual console step is a bug.** If `make up` can't create it, it doesn't exist.

Terraform state lives in S3 *inside the sandbox account*. Losing state is
normally a disaster; here the loss is **atomic with the resources**, so state
and reality stay consistent (both empty). This looks wrong at first glance —
it isn't.

## Prerequisites

- AWS credentials for the sandbox account (`AWS_PROFILE` or env vars)
- `terraform` >= 1.9, `uv`, `docker`, `databricks` CLI authenticated to the workspace
- `cp infra/envs/dev/terraform.tfvars.example infra/envs/dev/terraform.tfvars` and fill in
  `repo_url` and `kafka_client_cidrs` (the Databricks workspace NAT EIP plus your own IP, each `/32`)

## Local development

```bash
make compose-up        # single-broker Kafka on localhost:9092
make check             # lint + typecheck + unit tests
make test-integration  # end-to-end against the local broker
make compose-down
```

Run producers against local Kafka without touching AWS:

```bash
docker compose -f docker/compose.yaml --profile live up --build
```

## Topics

| Topic | Partitions | Retention |
|---|---|---|
| `md.trades.v1` | 6 | 24h |
| `md.book.top.v1` | 6 | 6h |
| `md.book.depth.v1` | 6 | 2h (off by default) |
| `md.bars.v1` | 3 | 48h |
| `news.articles.v1` | 3 | 7d |
| `ops.metrics.v1` | 1 | 7d |

Records are bare Avro datums (no registry, no magic byte) with the schema
version in a Kafka header. Producer and Spark reader load the same `.avsc` from
`ingest/schemas/`, so drift is impossible by construction.

## Known limitations

- **Teardown loses in-flight Kafka data by design.** `make down` destroys the
  brokers; anything not yet consumed into Bronze is gone. Backfill re-derives it
  from public archives and the reconciliation job proves it converged.
- **Binance gets exact-range gap repair; Coinbase gets best-effort.** Coinbase's
  public market-data API has no id-range query, so repair refetches recent
  trades and relies on natural-key dedupe. Not every venue supports the same
  repair, and hiding that would be worse than documenting it.
- **Equity coverage is IEX-only (~2% of volume)** on Alpaca's free tier. Crypto
  carries the real streaming workload.
```

- [ ] **Step 3: Verify the Makefile targets parse**

Run: `make -n up && make -n down && make -n smoke`
Expected: each prints its commands without executing

- [ ] **Step 4: Run the full local check**

Run: `make check`
Expected: lint, typecheck, and all unit tests pass

- [ ] **Step 5: Full end-to-end verification against real AWS**

Run: `time make up`
Expected: completes in under 20 minutes and ends with `SMOKE OK: 5 live trades decoded`

Then verify idempotence and the rebuild contract:

Run: `make down && time make up`
Expected: succeeds again from a clean account, same smoke result

- [ ] **Step 6: Commit**

```bash
git add Makefile README.md
git commit -m "feat: make up/down lifecycle targets and reproducibility README"
```

---

## Self-Review

**Spec coverage.** Walked §2–§5, §10, §12 and §13 stages 0–1 of the design doc:

| Spec requirement | Task |
|---|---|
| Two-account topology, Kafka as contract | 15, 17 |
| `kafka_backend` toggle | **Gap — see below** |
| Role ARNs as inputs never resources | 16 (`instance_profile_name`), verified in 13/15 |
| One-command rebuild under 20 min | 18, 19 |
| Terraform state atomic with resources | 13, 19 (README) |
| Topic table with partitions and retention | 18 |
| Avro without a registry; CI compat guard | 2 |
| Natural keys per venue | 3, 9, 10 |
| Gap detection and REST repair | 8, 9, 10, 11 |
| Backpressure: trades block, depth drops | 6 |
| Per-venue rate limiting | 4 |
| `config/universe.yaml` | 3 |
| Producer telemetry topic | 18 (`ops.metrics.v1` created; emission is stage 2) |
| Testing strategy: unit/contract/integration/infra/CI | 1, 2, 12, 13 |
| MSK 50 GB per broker | 15 |

**Gap found and accepted:** the `kafka_backend = "msk" | "ec2"` hedge from spec §2 has no
task. Adding a second, fully-tested Kafka backend before the primary one is proven would be
speculative work. The mitigation is cheap and real: `infra/modules/kafka_msk` is consumed
through a single `module "kafka"` block in `infra/envs/dev/main.tf` whose outputs
(`bootstrap_brokers_sasl_scram*`, `sasl_username`, `sasl_password`) are the entire contract —
so a `kafka_ec2` module is a drop-in swap with no change to `ingest/` or the bootstrap
script. **If Task 15's apply fails on an SCP, write the `kafka_ec2` module against that same
output contract before proceeding.** Recorded here rather than silently dropped.

**Placeholder scan.** One deliberate instance: Task 4 Step 3 ships a syntactically invalid
`acquire()` that Step 4 replaces. That is a teaching step with the real code supplied, not a
TBD. No other `TODO`, `TBD`, "similar to Task N", or "add error handling" instances.

**Type consistency.** Verified across tasks: `Trade` field names match `trade.v1.avsc`
exactly (Task 2 ↔ 3); `Gap(venue, venue_symbol, last_seen, next_seen)` is constructed
identically in Tasks 8, 9, 10, 11; `TradeProducer.produce/poll/flush` matches the
`ProducerLike` protocol in Task 11; `Connector` protocol members match both connector
implementations (asserted at runtime by `tests/connectors/test_protocol.py`);
`InstrumentMap.canonical/symbols_for` used consistently in Tasks 3, 9, 10, 11;
`TOPIC_POLICIES` keys match `TOPIC_SPECS` names across Tasks 6 and 18.

One naming asymmetry is intentional and documented: `CoinbaseConnector` exposes
`sequence_symbol = "*"` because Coinbase's `sequence_num` is connection-wide, and
`IngestRunner.handle_message` reads it via `getattr(..., trade.venue_symbol)` so Binance
needs no such attribute.
