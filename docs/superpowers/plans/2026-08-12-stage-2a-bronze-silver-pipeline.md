# Stage 2a Bronze + Silver Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Lakeflow Declarative Pipeline that reads `md.trades.v1` from MSK into `bronze_trades_stream`, then keyed-upserts validated rows into `silver_trades` (AUTO CDC, SCD1, CDF on) while diverting every rejected row to `silver_trades_quarantine`.

**Architecture:** All transformation logic lives in `lakehouse/trades/transforms.py` as plain PySpark functions that a local `pytest` suite exercises end to end, including real Avro decode. `lakehouse/pipelines/trades.py` is a thin declarative shell — the only module importing `from pyspark import pipelines as dp` (Databricks Runtime only), so the test suite never touches it. A Declarative Automation Bundle deploys the pipeline to `fdai.market`.

**Tech Stack:** Python 3.12, PySpark 3.5.3 (local tests) + `spark-avro_2.12:3.5.3`, Lakeflow Declarative Pipelines (`pyspark.pipelines`), Databricks Asset Bundles, uv, pytest, mypy strict, ruff.

## Global Constraints

- **Databricks target:** workspace `itoc-training-data-ai`, CLI profile `tw`. Always pass `--profile tw`; never rely on a default profile.
- **Catalog / schema:** `fdai` / `market` — both already created and owned by `lam.nguyen@thoughtworks.com`. Do not recreate.
- **Pipeline edition:** `ADVANCED` exactly. `PRO` has no CDC. Verified by dry run.
- **Compute:** `serverless: false` with a classic cluster, `node_type_id: m5d.large`, `num_workers: 1`. Serverless is forbidden — its egress IPs rotate and MSK is IP-allowlisted (spec §8).
- **`continuous`:** `false` (triggered). No schedule ships in this plan.
- **CDC flow name:** `cdc_trades_stream` exactly, so Stage 3a can add `cdc_trades_archive` to the same target additively.
- **CDC keys:** `["venue", "trade_id"]`. **`sequence_by`:** `"event_ts_us"`. **`stored_as_scd_type`:** `1`.
- **CDF:** `delta.enableChangeDataFeed = "true"` on `silver_trades` at creation. Enabling it later does not backfill change data.
- **Decimal spec:** `DECIMAL(38,18)` for `price` and `size`, cast from wire strings with `try_cast`.
- **Epoch floor:** `1483228800000000` (2017-01-01T00:00:00Z in microseconds). Ceiling: `unix_micros(current_timestamp()) + 86400000000`.
- **Avro:** bare datum, no schema registry, no magic-byte prefix. Single source of truth is `ingest/schemas/trade.v1.avsc` — never copy or re-declare it.
- **`from_avro` mode:** `PERMISSIVE`. `FAILFAST` would halt all ingestion on one poison record.
- **Local JDK:** `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` (openjdk 17.0.20, present but unlinked — `java` is not on PATH).
- **Out of scope:** the MSK region move to `ap-southeast-2` and the `kafka_client_cidrs` NAT-EIP allowlist are separate infra work the user will do later. Every deliverable here must be verifiable without a reachable broker.
- **Style:** ruff line-length 100, select `E,F,I,UP,B,ASYNC,RUF`; mypy strict; `from __future__ import annotations` at the top of every module. Comments explain *why*, matching the existing Makefile and `ingest/` culture.
- **Do not commit** `notebooks/04_stream_product_research.ipynb` — it holds the user's unrelated in-flight work.

---

### Task 1: Lakehouse package skeleton and local Spark harness

Nothing can be tested until a local SparkSession exists. This task delivers the harness plus the schema loader that every later task imports.

**Files:**
- Modify: `pyproject.toml` (packages list, new `lakehouse` dependency group)
- Create: `lakehouse/__init__.py`, `lakehouse/trades/__init__.py`
- Create: `lakehouse/trades/schema.py`
- Create: `tests/lakehouse/__init__.py`, `tests/lakehouse/conftest.py`
- Create: `tests/lakehouse/test_schema.py`
- Modify: `Makefile` (add `lakehouse-test`)

**Interfaces:**
- Consumes: `ingest/schemas/trade.v1.avsc` (existing file, do not modify).
- Produces:
  - `lakehouse.trades.schema.trade_avsc_path() -> pathlib.Path`
  - `lakehouse.trades.schema.trade_avsc_json() -> str` — the raw JSON text `from_avro` needs
  - `lakehouse.trades.schema.EPOCH_FLOOR_US: int = 1483228800000000`
  - `lakehouse.trades.schema.ONE_DAY_US: int = 86400000000`
  - `lakehouse.trades.schema.DECIMAL_TYPE: str = "DECIMAL(38,18)"`
  - `lakehouse.trades.schema.SILVER_COLUMNS: list[str]`
  - `lakehouse.trades.schema.BRONZE_AUDIT_COLUMNS: list[str]`
  - `tests/lakehouse/conftest.py` fixtures: `spark` (session-scoped `SparkSession`), `trade_record` (callable builder), `avro_bytes` (callable encoder)

- [ ] **Step 1: Add the package and dependency group to pyproject.toml**

In `[tool.setuptools.packages.find]`, change the `include` line to add `lakehouse*`:

```toml
include = ["ingest*", "backfill*", "devlab*", "lakehouse*"]
```

In `[dependency-groups]`, add a new group after `notebook`:

```toml
# Not a default group: pyspark is ~300MB and only the lakehouse tests need it.
# The pipeline itself runs on Databricks Runtime, which supplies its own Spark,
# so this pin only has to agree with DBR closely enough for SQL semantics.
# Opt in with `uv sync --group lakehouse` or `make lakehouse-test`.
lakehouse = [
    "pyspark==3.5.3",
]
```

- [ ] **Step 2: Create the package `__init__.py` files**

`lakehouse/__init__.py`:

```python
"""Databricks lakehouse code: Bronze/Silver transforms and pipeline definitions.

Transformation logic lives in `lakehouse.trades.transforms` as plain PySpark so
it is testable on a laptop. `lakehouse.pipelines.*` holds the declarative shells
that only import cleanly on Databricks Runtime.
"""
```

`lakehouse/trades/__init__.py`:

```python
"""Trade-stream transforms shared by the Bronze/Silver pipeline and its tests."""
```

- [ ] **Step 3: Write the failing test for the schema loader**

Create `tests/lakehouse/__init__.py` as an empty file, then `tests/lakehouse/test_schema.py`:

```python
from __future__ import annotations

import json

from lakehouse.trades import schema


def test_avsc_path_points_at_the_producer_schema():
    # Single source of truth: the consumer must read the very file the producer
    # encodes with, or the "drift is impossible" property in codec.py is lost.
    path = schema.trade_avsc_path()
    assert path.exists()
    assert path.as_posix().endswith("ingest/schemas/trade.v1.avsc")


def test_avsc_json_parses_and_declares_the_expected_fields():
    doc = json.loads(schema.trade_avsc_json())
    assert doc["name"] == "Trade"
    names = [f["name"] for f in doc["fields"]]
    assert names == [
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        "ingest_ts_us",
        "price",
        "size",
        "side",
        "sequence",
        "is_backfill",
        "source",
    ]


def test_epoch_floor_rejects_millisecond_timestamps():
    # A 2026 timestamp in milliseconds is ~1.78e12, three orders of magnitude
    # below the microsecond floor. This constant is the ms/us tripwire.
    ms_style = 1_786_000_000_000
    assert ms_style < schema.EPOCH_FLOOR_US


def test_silver_columns_exclude_bronze_audit_columns():
    assert set(schema.SILVER_COLUMNS).isdisjoint(schema.BRONZE_AUDIT_COLUMNS)
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `uv run --group lakehouse pytest tests/lakehouse/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'lakehouse.trades.schema'`

- [ ] **Step 5: Implement the schema module**

Create `lakehouse/trades/schema.py`:

```python
"""Constants and the Avro schema loader shared by transforms and pipeline.

The .avsc is read from the repo rather than re-declared here. On Databricks the
bundle syncs the whole repo, so the same relative path resolves there too; the
named fallback if it ever does not is injecting the JSON through pipeline
configuration (design doc B3).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

# 2017-01-01T00:00:00Z in microseconds. Chosen as a floor because it predates
# any data this project ingests while still sitting ~1000x above any timestamp
# accidentally left in milliseconds.
EPOCH_FLOOR_US = 1_483_228_800_000_000
ONE_DAY_US = 86_400_000_000

# 18 fractional digits is below satoshi granularity; 20 integral digits is
# beyond any plausible price. The wire format carries both as strings so no
# float ever touches a price.
DECIMAL_TYPE = "DECIMAL(38,18)"

QUARANTINE_REASON = "_quarantine_reason"

# Columns Bronze keeps for forensics and Silver deliberately drops. Carrying
# Kafka offsets into Silver would break the design's §2.3 argument that the
# stream and archive copies of a trade are interchangeable.
BRONZE_AUDIT_COLUMNS = [
    "_kafka_value",
    "_kafka_key",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_ingested_at",
    QUARANTINE_REASON,
]

SILVER_COLUMNS = [
    "venue",
    "venue_symbol",
    "instrument_id",
    "trade_id",
    "event_ts_us",
    "event_ts",
    "ingest_ts_us",
    "price",
    "size",
    "side",
    "source",
    "sequence",
    "is_backfill",
]


def trade_avsc_path() -> Path:
    """Absolute path to the producer's trade schema.

    schema.py -> trades/ -> lakehouse/ -> repo root, then down into ingest/.
    """
    return Path(__file__).resolve().parents[2] / "ingest" / "schemas" / "trade.v1.avsc"


@lru_cache(maxsize=1)
def trade_avsc_json() -> str:
    """Raw JSON text, which is the form from_avro's jsonFormatSchema expects."""
    return trade_avsc_path().read_text(encoding="utf-8")
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run --group lakehouse pytest tests/lakehouse/test_schema.py -v`
Expected: 4 passed

- [ ] **Step 7: Write the shared Spark fixtures**

Create `tests/lakehouse/conftest.py`:

```python
"""Local Spark harness for the lakehouse tests.

Two things make this work offline. JAVA_HOME is set explicitly because openjdk
17 is installed via brew but not linked, so `java` is not on PATH. And spark-avro
is pulled as a Maven package because the pyspark wheel does not bundle it —
without it `from_avro` raises "'JavaPackage' object is not callable". The jar is
cached under ~/.ivy2 after the first resolution, so later runs need no network.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Callable
from typing import Any

import pytest

JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

BASE_TS_US = 1_700_000_000_000_000  # 2023-11-14T22:13:20Z, same anchor as tests/devlab


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark", reason="needs `uv sync --group lakehouse`")
    from pyspark.sql import SparkSession

    os.environ.setdefault("JAVA_HOME", JAVA_HOME)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", f"org.apache.spark:spark-avro_2.12:{pyspark.__version__}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _record(
    *,
    venue: str = "binance",
    venue_symbol: str = "BTCUSDT",
    instrument_id: str = "BTC-USD",
    trade_id: str = "42",
    event_ts_us: int = BASE_TS_US,
    ingest_ts_us: int | None = None,
    price: str = "100.5",
    size: str = "0.25",
    side: str = "BUY",
    sequence: int | None = 7,
    is_backfill: bool = False,
    source: str = "STREAM",
) -> dict[str, Any]:
    """One trade record shaped exactly as ingest.core.models.Trade.to_avro yields."""
    return {
        "venue": venue,
        "venue_symbol": venue_symbol,
        "instrument_id": instrument_id,
        "trade_id": trade_id,
        "event_ts_us": event_ts_us,
        "ingest_ts_us": ingest_ts_us if ingest_ts_us is not None else event_ts_us + 250_000,
        "price": price,
        "size": size,
        "side": side,
        "sequence": sequence,
        "is_backfill": is_backfill,
        "source": source,
    }


@pytest.fixture
def trade_record() -> Callable[..., dict[str, Any]]:
    return _record


@pytest.fixture
def avro_bytes() -> Callable[[dict[str, Any]], bytes]:
    """Encode a record with the same codec the producer uses, so tests exercise
    the real wire format rather than a hand-built approximation."""
    import fastavro

    from lakehouse.trades.schema import trade_avsc_json

    parsed = fastavro.parse_schema(json.loads(trade_avsc_json()))

    def encode(record: dict[str, Any]) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed, record)
        return buf.getvalue()

    return encode
```

- [ ] **Step 8: Add the Makefile target**

Append to `Makefile`:

```makefile
.PHONY: lakehouse-test

# pyspark needs a JDK, and openjdk@17 is installed via brew but not linked, so
# `java` is not on PATH. Setting JAVA_HOME here keeps that detail out of every
# developer's shell profile. spark-avro is fetched from Maven on first run and
# cached in ~/.ivy2 afterwards.
JAVA_HOME_17 ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

lakehouse-test:
	JAVA_HOME=$(JAVA_HOME_17) uv run --group lakehouse pytest tests/lakehouse -v
```

- [ ] **Step 9: Verify the harness starts a real session**

Run: `make lakehouse-test`
Expected: 4 passed. (First run downloads pyspark and the spark-avro jar; allow several minutes.)

- [ ] **Step 10: Commit**

```bash
git add pyproject.toml Makefile lakehouse tests/lakehouse
git commit -m "feat(lakehouse): add package skeleton and local Spark test harness"
```

---

### Task 2: Avro decode into Bronze columns

**Files:**
- Create: `lakehouse/trades/transforms.py`
- Create: `tests/lakehouse/test_decode.py`

**Interfaces:**
- Consumes: `lakehouse.trades.schema.trade_avsc_json`, `BRONZE_AUDIT_COLUMNS`.
- Produces: `lakehouse.trades.transforms.decode_kafka_trades(df: DataFrame) -> DataFrame`. Input must have Kafka's `key, value, topic, partition, offset, timestamp` columns. Output has every decoded Avro field flattened to top level, plus the `_kafka_*` and `_ingested_at` audit columns. A record that fails to decode yields NULLs in the Avro fields and keeps its `_kafka_value`.

- [ ] **Step 1: Write the failing decode tests**

Create `tests/lakehouse/test_decode.py`:

```python
from __future__ import annotations

from lakehouse.trades.transforms import decode_kafka_trades


def _kafka_frame(spark, rows):
    """Mimic the shape spark.readStream.format('kafka') produces."""
    return spark.createDataFrame(
        rows,
        "key binary, value binary, topic string, partition int, "
        "offset long, timestamp timestamp",
    )


def test_decodes_a_real_producer_datum(spark, trade_record, avro_bytes):
    import datetime as dt

    rec = trade_record(trade_id="99", price="12345.678", side="SELL")
    df = _kafka_frame(
        spark,
        [
            (
                bytearray(b"binance|BTCUSDT"),
                bytearray(avro_bytes(rec)),
                "md.trades.v1",
                3,
                1234,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
    )
    out = decode_kafka_trades(df).collect()[0]

    assert out["venue"] == "binance"
    assert out["trade_id"] == "99"
    assert out["price"] == "12345.678"  # still a string in Bronze
    assert out["side"] == "SELL"
    assert out["source"] == "STREAM"
    assert out["sequence"] == 7
    assert out["is_backfill"] is False
    # Kafka metadata is preserved for forensics.
    assert out["_kafka_topic"] == "md.trades.v1"
    assert out["_kafka_partition"] == 3
    assert out["_kafka_offset"] == 1234
    assert out["_kafka_key"] == "binance|BTCUSDT"
    assert out["_ingested_at"] is not None


def test_corrupt_datum_yields_nulls_instead_of_failing(spark):
    import datetime as dt

    # PERMISSIVE is the whole point: one poison record must not abort the batch.
    df = _kafka_frame(
        spark,
        [
            (
                bytearray(b"k"),
                bytearray(b"\xff\xff\xff\xff\xff\xff"),
                "md.trades.v1",
                0,
                1,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
    )
    out = decode_kafka_trades(df).collect()[0]

    assert out["venue"] is None
    assert out["trade_id"] is None
    # The raw bytes survive so the record stays diagnosable.
    assert out["_kafka_value"] is not None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make lakehouse-test`
Expected: FAIL with `ImportError: cannot import name 'decode_kafka_trades'`

- [ ] **Step 3: Implement decode**

Create `lakehouse/trades/transforms.py`:

```python
"""Pure PySpark transforms for the trades Bronze/Silver path.

Every function here takes and returns a DataFrame and touches no Databricks-only
API, which is what lets the whole path be tested on a laptop. The declarative
pipeline in lakehouse/pipelines/trades.py is a thin shell over these.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from lakehouse.trades.schema import trade_avsc_json

# The producer writes a bare Avro datum via fastavro.schemaless_writer, with no
# Confluent magic-byte prefix and no schema registry, which is exactly what
# from_avro's jsonFormatSchema form consumes. See ingest/core/codec.py.
#
# PERMISSIVE, not the default FAILFAST: FAILFAST aborts the entire micro-batch
# on a single malformed record, so one poison message halts ingestion forever.
# PERMISSIVE yields a NULL struct, which classify_trades turns into a
# quarantined row.
_AVRO_OPTIONS = {"mode": "PERMISSIVE"}

_DECODED = "_decoded"


def decode_kafka_trades(df: DataFrame) -> DataFrame:
    """Decode Kafka's binary `value` into flat trade columns, keeping audit data."""
    return (
        df.withColumn(_DECODED, from_avro(F.col("value"), trade_avsc_json(), _AVRO_OPTIONS))
        .select(
            F.col(f"{_DECODED}.*"),
            F.col("value").alias("_kafka_value"),
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make lakehouse-test`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add lakehouse/trades/transforms.py tests/lakehouse/test_decode.py
git commit -m "feat(lakehouse): decode bare Avro trade datums into Bronze columns"
```

---

### Task 3: Quarantine classification

The heart of the design's §2.1 correction: validation is an explicit branch, not an expectation, so no row is ever dropped.

**Files:**
- Modify: `lakehouse/trades/transforms.py`
- Create: `tests/lakehouse/test_classify.py`

**Interfaces:**
- Consumes: `decode_kafka_trades` output, `schema.EPOCH_FLOOR_US`, `ONE_DAY_US`, `DECIMAL_TYPE`, `QUARANTINE_REASON`.
- Produces:
  - `lakehouse.trades.transforms.QUARANTINE_PREDICATES: dict[str, str]` — reason name to the SQL predicate that means *valid*, in evaluation order. Reused by the pipeline shell to declare warn-only expectations.
  - `lakehouse.trades.transforms.classify_trades(df: DataFrame) -> DataFrame` — adds a nullable `_quarantine_reason` string column. NULL means the row is valid.

- [ ] **Step 1: Write the failing classification tests**

Create `tests/lakehouse/test_classify.py`:

```python
from __future__ import annotations

import datetime as dt

import pytest

from lakehouse.trades.schema import QUARANTINE_REASON
from lakehouse.trades.transforms import classify_trades, decode_kafka_trades


def _reason(spark, avro_bytes, record):
    df = spark.createDataFrame(
        [
            (
                bytearray(b"k"),
                bytearray(avro_bytes(record)),
                "md.trades.v1",
                0,
                1,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
        "key binary, value binary, topic string, partition int, "
        "offset long, timestamp timestamp",
    )
    return classify_trades(decode_kafka_trades(df)).collect()[0][QUARANTINE_REASON]


def test_a_good_trade_has_no_reason(spark, trade_record, avro_bytes):
    assert _reason(spark, avro_bytes, trade_record()) is None


def test_corrupt_datum_is_decode_failed(spark):
    df = spark.createDataFrame(
        [
            (
                bytearray(b"k"),
                bytearray(b"\xff\xff\xff\xff\xff\xff"),
                "md.trades.v1",
                0,
                1,
                dt.datetime(2026, 8, 12, 1, 2, 3),
            )
        ],
        "key binary, value binary, topic string, partition int, "
        "offset long, timestamp timestamp",
    )
    out = classify_trades(decode_kafka_trades(df)).collect()[0]
    assert out[QUARANTINE_REASON] == "decode_failed"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"venue": ""}, "missing_key"),
        ({"trade_id": ""}, "missing_key"),
        ({"instrument_id": ""}, "missing_instrument"),
        # A 2026 timestamp left in milliseconds -- the §6.3 unit trap.
        ({"event_ts_us": 1_786_000_000_000}, "bad_timestamp"),
        ({"price": "not-a-number"}, "bad_price"),
        ({"price": "0"}, "bad_price"),
        ({"price": "-1.5"}, "bad_price"),
        ({"size": "not-a-number"}, "bad_size"),
        ({"size": "0"}, "bad_size"),
    ],
)
def test_each_rejection_reports_its_own_reason(
    spark, trade_record, avro_bytes, overrides, expected
):
    assert _reason(spark, avro_bytes, trade_record(**overrides)) == expected


def test_future_timestamp_beyond_one_day_is_rejected(spark, trade_record, avro_bytes):
    far_future = int((dt.datetime.now(dt.UTC).timestamp() + 3 * 86_400) * 1_000_000)
    assert _reason(spark, avro_bytes, trade_record(event_ts_us=far_future)) == "bad_timestamp"


def test_first_matching_reason_wins(spark, trade_record, avro_bytes):
    # Both the key and the price are broken; the more fundamental one reports.
    record = trade_record(trade_id="", price="nope")
    assert _reason(spark, avro_bytes, record) == "missing_key"


def test_high_precision_price_is_not_rejected(spark, trade_record, avro_bytes):
    # 18 fractional digits must survive; rejecting it would silently drop
    # legitimate small-tick instruments.
    assert _reason(spark, avro_bytes, trade_record(price="1.234567890123456789")) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make lakehouse-test`
Expected: FAIL with `ImportError: cannot import name 'classify_trades'`

- [ ] **Step 3: Implement classification**

Append to `lakehouse/trades/transforms.py`:

```python
# Each entry maps a reason name to the SQL predicate that must hold for a row to
# be VALID. Order matters: the first failing predicate names the reason, so the
# most fundamental problem is reported rather than a downstream symptom.
#
# The pipeline shell reuses these as warn-only expectations, which is what keeps
# the DLT quality metrics behind the quarantine-rate SLI meaningful while the
# actual routing is done by the reason column.
QUARANTINE_PREDICATES: dict[str, str] = {
    # A NULL struct is what PERMISSIVE from_avro produces for a bad datum.
    "decode_failed": "venue IS NOT NULL OR trade_id IS NOT NULL OR event_ts_us IS NOT NULL",
    # AUTO CDC keys cannot be NULL, so this is a pipeline error, not a warning.
    "missing_key": (
        "venue IS NOT NULL AND venue <> '' AND trade_id IS NOT NULL AND trade_id <> ''"
    ),
    "missing_instrument": "instrument_id IS NOT NULL AND instrument_id <> ''",
    "bad_timestamp": (
        f"event_ts_us IS NOT NULL AND event_ts_us >= {EPOCH_FLOOR_US} "
        f"AND event_ts_us <= unix_micros(current_timestamp()) + {ONE_DAY_US}"
    ),
    "bad_price": f"try_cast(price AS {DECIMAL_TYPE}) > 0",
    "bad_size": f"try_cast(size AS {DECIMAL_TYPE}) > 0",
    "bad_side": "side IN ('BUY', 'SELL', 'UNKNOWN')",
}


def classify_trades(df: DataFrame) -> DataFrame:
    """Add a nullable `_quarantine_reason`; NULL means the row is valid."""
    chain = F
    expr = None
    for reason, valid_when in QUARANTINE_PREDICATES.items():
        branch = F.when(~F.expr(valid_when), F.lit(reason))
        expr = branch if expr is None else expr.otherwise(branch)
    assert expr is not None  # QUARANTINE_PREDICATES is never empty
    del chain
    return df.withColumn(QUARANTINE_REASON, expr)
```

Then extend the import at the top of the module:

```python
from lakehouse.trades.schema import (
    DECIMAL_TYPE,
    EPOCH_FLOOR_US,
    ONE_DAY_US,
    QUARANTINE_REASON,
    trade_avsc_json,
)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make lakehouse-test`
Expected: 19 passed

If `bad_price` reports where `missing_key` is expected, the `when/otherwise` chain is nesting wrongly — each `when` must chain onto the previous `otherwise`, not onto `F`.

- [ ] **Step 5: Remove the leftover scaffolding**

Delete the `chain = F` and `del chain` lines — they are noise from drafting. Re-run `make lakehouse-test` and confirm 19 still pass.

- [ ] **Step 6: Commit**

```bash
git add lakehouse/trades/transforms.py tests/lakehouse/test_classify.py
git commit -m "feat(lakehouse): classify invalid trades with an ordered quarantine reason"
```

---

### Task 4: Silver projection

**Files:**
- Modify: `lakehouse/trades/transforms.py`
- Create: `tests/lakehouse/test_silver.py`

**Interfaces:**
- Consumes: `classify_trades` output, `schema.SILVER_COLUMNS`, `DECIMAL_TYPE`.
- Produces:
  - `lakehouse.trades.transforms.valid_trades(df: DataFrame) -> DataFrame` — rows where the reason is NULL, projected to exactly `SILVER_COLUMNS` with typed `price`/`size` and a derived `event_ts`.
  - `lakehouse.trades.transforms.quarantined_trades(df: DataFrame) -> DataFrame` — rows where the reason is NOT NULL, keeping the reason and all audit columns.

- [ ] **Step 1: Write the failing Silver tests**

Create `tests/lakehouse/test_silver.py`:

```python
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from lakehouse.trades.schema import QUARANTINE_REASON, SILVER_COLUMNS
from lakehouse.trades.transforms import (
    classify_trades,
    decode_kafka_trades,
    quarantined_trades,
    valid_trades,
)

_KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _classified(spark, avro_bytes, records):
    rows = [
        (
            bytearray(b"k"),
            bytearray(avro_bytes(r)),
            "md.trades.v1",
            0,
            i,
            dt.datetime(2026, 8, 12, 1, 2, 3),
        )
        for i, r in enumerate(records)
    ]
    return classify_trades(decode_kafka_trades(spark.createDataFrame(rows, _KAFKA_SCHEMA)))


def test_silver_projects_exactly_the_contract_columns(spark, trade_record, avro_bytes):
    out = valid_trades(_classified(spark, avro_bytes, [trade_record()]))
    assert out.columns == SILVER_COLUMNS


def test_silver_drops_kafka_metadata(spark, trade_record, avro_bytes):
    # Carrying offsets into Silver would break the interchangeability argument
    # that makes SCD Type 1 safe for the archive backfill (design §2.3).
    out = valid_trades(_classified(spark, avro_bytes, [trade_record()]))
    assert not [c for c in out.columns if c.startswith("_kafka")]
    assert QUARANTINE_REASON not in out.columns


def test_price_and_size_become_exact_decimals(spark, trade_record, avro_bytes):
    record = trade_record(price="1.234567890123456789", size="0.000000000000000001")
    row = valid_trades(_classified(spark, avro_bytes, [record])).collect()[0]
    assert row["price"] == Decimal("1.234567890123456789")
    assert row["size"] == Decimal("0.000000000000000001")


def test_event_ts_is_derived_from_microseconds(spark, trade_record, avro_bytes):
    record = trade_record(event_ts_us=1_700_000_000_000_000)
    row = valid_trades(_classified(spark, avro_bytes, [record])).collect()[0]
    assert row["event_ts"] == dt.datetime(2023, 11, 14, 22, 13, 20)
    assert row["event_ts_us"] == 1_700_000_000_000_000


def test_invalid_rows_are_absent_from_silver_and_present_in_quarantine(
    spark, trade_record, avro_bytes
):
    good = trade_record(trade_id="good")
    bad = trade_record(trade_id="bad", price="nope")
    classified = _classified(spark, avro_bytes, [good, bad])

    silver_ids = [r["trade_id"] for r in valid_trades(classified).collect()]
    assert silver_ids == ["good"]

    quarantined = quarantined_trades(classified).collect()
    assert len(quarantined) == 1
    assert quarantined[0]["trade_id"] == "bad"
    assert quarantined[0][QUARANTINE_REASON] == "bad_price"
    # Nothing is dropped: the raw bytes are still there to diagnose.
    assert quarantined[0]["_kafka_value"] is not None


def test_quarantine_keeps_the_reason_column(spark, trade_record, avro_bytes):
    classified = _classified(spark, avro_bytes, [trade_record(size="0")])
    assert QUARANTINE_REASON in quarantined_trades(classified).columns
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make lakehouse-test`
Expected: FAIL with `ImportError: cannot import name 'valid_trades'`

- [ ] **Step 3: Implement the two branches**

Append to `lakehouse/trades/transforms.py`:

```python
def valid_trades(df: DataFrame) -> DataFrame:
    """The clean branch: typed, projected to the Silver contract, nothing extra.

    Kafka audit columns are dropped here rather than by the CDC flow's
    except_column_list so that the projection is testable without Databricks.
    The flow still declares the exclusions, which is belt and braces.
    """
    return df.where(F.col(QUARANTINE_REASON).isNull()).select(
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        F.timestamp_micros(F.col("event_ts_us")).alias("event_ts"),
        "ingest_ts_us",
        F.col("price").cast(DECIMAL_TYPE).alias("price"),
        F.col("size").cast(DECIMAL_TYPE).alias("size"),
        "side",
        "source",
        "sequence",
        "is_backfill",
    )


def quarantined_trades(df: DataFrame) -> DataFrame:
    """The rejected branch: everything, plus why it was rejected.

    Deliberately keeps `_kafka_value` — a quarantined record that cannot be
    re-read is a lost record with a receipt.
    """
    return df.where(F.col(QUARANTINE_REASON).isNotNull())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make lakehouse-test`
Expected: 25 passed

- [ ] **Step 5: Verify the projection matches the declared contract**

The `test_silver_projects_exactly_the_contract_columns` assertion couples the code to `SILVER_COLUMNS`. If it fails on ordering, fix the `select` order to match `SILVER_COLUMNS` — the constant is the contract, not the code.

- [ ] **Step 6: Commit**

```bash
git add lakehouse/trades/transforms.py tests/lakehouse/test_silver.py
git commit -m "feat(lakehouse): project validated trades to the Silver contract"
```

---

### Task 5: Declarative pipeline shell and its offline contract test

The shell cannot be imported off-platform, so it is verified by parsing it. That catches exactly the drift that would silently break dedupe.

**Files:**
- Create: `lakehouse/pipelines/__init__.py`, `lakehouse/pipelines/trades.py`
- Create: `tests/lakehouse/test_pipeline_contract.py`

**Interfaces:**
- Consumes: every transform from Tasks 2-4, `QUARANTINE_PREDICATES`.
- Produces: three datasets — `bronze_trades_stream`, `silver_trades`, `silver_trades_quarantine` — and one CDC flow named `cdc_trades_stream`.

- [ ] **Step 1: Write the failing contract test**

Create `tests/lakehouse/test_pipeline_contract.py`:

```python
"""Offline contract tests for the pipeline shell.

The shell imports `pyspark.pipelines`, which exists only on Databricks Runtime,
so it cannot be imported here. Parsing it instead still pins the values whose
drift would be invisible until it corrupted data: the CDC keys, the sequence
column, the SCD type, the flow name, and Change Data Feed.
"""

from __future__ import annotations

import ast
from pathlib import Path

PIPELINE = Path("lakehouse/pipelines/trades.py")


def _tree() -> ast.Module:
    return ast.parse(PIPELINE.read_text(encoding="utf-8"))


def _call(name: str) -> ast.Call:
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Call):
            func = node.func
            attr = func.attr if isinstance(func, ast.Attribute) else None
            if attr == name:
                return node
    raise AssertionError(f"no call to {name} found in {PIPELINE}")


def _kwarg(call: ast.Call, name: str) -> object:
    for kw in call.keywords:
        if kw.arg == name:
            return ast.literal_eval(kw.value)
    raise AssertionError(f"{name} not passed")


def test_pipeline_file_exists():
    assert PIPELINE.exists()


def test_cdc_flow_pins_the_dedupe_contract():
    call = _call("create_auto_cdc_flow")
    # Renaming the flow would orphan its checkpoint; changing keys or the
    # sequence column would silently change which duplicate wins.
    assert _kwarg(call, "name") == "cdc_trades_stream"
    assert _kwarg(call, "target") == "silver_trades"
    assert _kwarg(call, "keys") == ["venue", "trade_id"]
    assert _kwarg(call, "sequence_by") == "event_ts_us"
    assert _kwarg(call, "stored_as_scd_type") == 1


def test_silver_enables_change_data_feed():
    # Stage 2b's scoped recompute reads CDF. Enabling it after the table has
    # data does not produce change data for existing commits.
    call = _call("create_streaming_table")
    props = _kwarg(call, "table_properties")
    assert props["delta.enableChangeDataFeed"] == "true"


def test_cdc_flow_excludes_bronze_audit_columns():
    from lakehouse.trades.schema import BRONZE_AUDIT_COLUMNS

    excluded = _kwarg(_call("create_auto_cdc_flow"), "except_column_list")
    assert set(excluded) == set(BRONZE_AUDIT_COLUMNS)


def test_shell_holds_no_business_logic():
    # Any predicate or cast here would be untested, since this file cannot run
    # under pytest. Logic belongs in transforms.py.
    source = PIPELINE.read_text(encoding="utf-8")
    for banned in ("try_cast", "when(", "EPOCH_FLOOR", "DECIMAL(38,18)"):
        assert banned not in source, f"{banned} belongs in transforms.py"


def test_tests_never_import_the_shell():
    # If any test imported it, the suite would fail off-platform.
    for path in Path("tests/lakehouse").glob("*.py"):
        assert "lakehouse.pipelines" not in path.read_text(encoding="utf-8") or (
            path.name == "test_pipeline_contract.py"
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make lakehouse-test`
Expected: FAIL — `test_pipeline_file_exists` fails, and `_call` raises `AssertionError` / `FileNotFoundError`.

- [ ] **Step 3: Write the pipeline shell**

Create `lakehouse/pipelines/__init__.py`:

```python
"""Declarative pipeline shells. These import Databricks-Runtime-only APIs."""
```

Create `lakehouse/pipelines/trades.py`:

```python
"""Bronze + Silver trades pipeline (stage 2a).

Kafka -> bronze_trades_stream -> (validated) -> silver_trades
                                            \\-> silver_trades_quarantine

This file is a shell. All logic lives in lakehouse.trades.transforms so it can
be tested without Databricks; see tests/lakehouse. `pyspark.pipelines` only
exists on Databricks Runtime, so nothing here is importable under pytest, and
tests/lakehouse/test_pipeline_contract.py pins these values by parsing instead.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.trades.schema import BRONZE_AUDIT_COLUMNS
from lakehouse.trades.transforms import (
    QUARANTINE_PREDICATES,
    classify_trades,
    decode_kafka_trades,
    quarantined_trades,
    valid_trades,
)

spark = SparkSession.active()

_SECRET_SCOPE = spark.conf.get("fdai.secret_scope", "fdai")
_TOPIC = spark.conf.get("fdai.kafka_topic", "md.trades.v1")
_STARTING_OFFSETS = spark.conf.get("fdai.starting_offsets", "earliest")
_MAX_OFFSETS_PER_TRIGGER = spark.conf.get("fdai.max_offsets_per_trigger", "1000000")


def _kafka_options() -> dict[str, str]:
    """SASL/SCRAM against the MSK public listener.

    The login-module class name must carry the `kafkashaded.` prefix: Databricks
    shades its Kafka client, and the unshaded name fails with
    RESTRICTED_STREAMING_OPTION_PERMISSION_ENFORCED.
    """
    username = dbutils.secrets.get(_SECRET_SCOPE, "kafka_username")  # noqa: F821
    password = dbutils.secrets.get(_SECRET_SCOPE, "kafka_password")  # noqa: F821
    bootstrap = dbutils.secrets.get(_SECRET_SCOPE, "kafka_bootstrap")  # noqa: F821
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{username}" password="{password}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": _TOPIC,
        "startingOffsets": _STARTING_OFFSETS,
        "maxOffsetsPerTrigger": _MAX_OFFSETS_PER_TRIGGER,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": jaas,
        # Left at the default `true` on purpose: silently skipping expired
        # offsets would hide real data loss. Recovery after the weekly sandbox
        # wipe is a SELECTIVE refresh of this table -- never the whole pipeline,
        # which would also reset silver_trades. See docs/RUNBOOK_STAGE_2A.md.
        "failOnDataLoss": "true",
    }


@dp.table(
    name="bronze_trades_stream",
    comment="Raw md.trades.v1: decoded Avro plus Kafka metadata and the original bytes.",
)
def bronze_trades_stream():
    raw = spark.readStream.format("kafka").options(**_kafka_options()).load()
    return decode_kafka_trades(raw)


# Warn-only, so no row is ever dropped here -- routing is done by
# _quarantine_reason. These exist purely so the quarantine rate is visible in
# the pipeline's data-quality metrics.
@dp.temporary_view(name="trades_validated")
@dp.expect_all({f"valid_{reason}": predicate for reason, predicate in QUARANTINE_PREDICATES.items()})
def trades_validated():
    return classify_trades(spark.readStream.table("bronze_trades_stream"))


@dp.table(
    name="silver_trades_quarantine",
    comment="Rejected records with the reason and the raw bytes needed to diagnose them.",
)
def silver_trades_quarantine():
    return quarantined_trades(spark.readStream.table("trades_validated"))


@dp.temporary_view(name="trades_clean")
def trades_clean():
    return valid_trades(spark.readStream.table("trades_validated"))


dp.create_streaming_table(
    name="silver_trades",
    comment="Deduplicated trade facts, keyed upsert on (venue, trade_id).",
    table_properties={
        # Stage 2b reads this to find dirty partitions. It must be on from the
        # first commit; enabling it later does not backfill change data.
        "delta.enableChangeDataFeed": "true",
    },
)

# Named flow from day one so stage 3a can add `cdc_trades_archive` into the same
# target without touching this one. SCD Type 1 is safe here because a trade is
# immutable: the stream and archive copies tie on event_ts_us.
dp.create_auto_cdc_flow(
    name="cdc_trades_stream",
    target="silver_trades",
    source="trades_clean",
    keys=["venue", "trade_id"],
    sequence_by="event_ts_us",
    stored_as_scd_type=1,
    except_column_list=BRONZE_AUDIT_COLUMNS,
)

del F  # imported for parity with transforms; not used in the shell
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make lakehouse-test`
Expected: 32 passed

- [ ] **Step 5: Remove the unused import**

Delete both the `from pyspark.sql import functions as F` line and the trailing `del F` line — an unused import that ruff would flag. Re-run `make lakehouse-test` (32 passed) and `uv run ruff check lakehouse`.

- [ ] **Step 6: Commit**

```bash
git add lakehouse/pipelines tests/lakehouse/test_pipeline_contract.py
git commit -m "feat(lakehouse): add trades pipeline shell with parsed contract tests"
```

---

### Task 6: Asset bundle and deployment targets

**Files:**
- Create: `databricks.yml`
- Create: `resources/trades.pipeline.yml`
- Modify: `Makefile`
- Create: `tests/lakehouse/test_bundle.py`

**Interfaces:**
- Consumes: `lakehouse/` sources.
- Produces: a bundle named `finance-data-ai` with a `dev` target and a pipeline resource keyed `trades_bronze_silver`.

- [ ] **Step 1: Write the failing bundle test**

Create `tests/lakehouse/test_bundle.py`:

```python
"""Static checks on the bundle. `databricks bundle validate` is the live check
(see `make pipeline-validate`); these run with no network and no auth."""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")


def _load(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def test_pipeline_pins_the_non_negotiable_settings():
    pipeline = _load("resources/trades.pipeline.yml")
    settings = pipeline["resources"]["pipelines"]["trades_bronze_silver"]

    assert settings["catalog"] == "fdai"
    assert settings["schema"] == "market"
    # ADVANCED is required for CDC; PRO does not include it.
    assert settings["edition"] == "ADVANCED"
    # Serverless egress IPs rotate, and MSK is IP-allowlisted.
    assert settings["serverless"] is False
    assert settings["continuous"] is False
    assert settings["clusters"][0]["node_type_id"] == "m5d.large"


def test_bundle_declares_the_dev_target_and_profile():
    bundle = _load("databricks.yml")
    assert bundle["bundle"]["name"] == "finance-data-ai"
    assert "dev" in bundle["targets"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make lakehouse-test`
Expected: FAIL with `FileNotFoundError: resources/trades.pipeline.yml`

- [ ] **Step 3: Write the bundle root**

Create `databricks.yml`:

```yaml
# Declarative Automation Bundle for the finance-data-ai lakehouse.
#
# The whole repo is synced, not just lakehouse/, because
# lakehouse/trades/schema.py reads ingest/schemas/trade.v1.avsc at runtime --
# the same file the producer encodes with. Duplicating the schema instead would
# reintroduce exactly the drift codec.py was written to make impossible.
bundle:
  name: finance-data-ai

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://itoc-training-data-ai.cloud.databricks.com
```

- [ ] **Step 4: Write the pipeline resource**

Create `resources/trades.pipeline.yml`:

```yaml
resources:
  pipelines:
    trades_bronze_silver:
      name: fdai_trades_bronze_silver
      catalog: fdai
      schema: market

      # ADVANCED, not PRO: AUTO CDC and SCD Type 1 are ADVANCED-only features.
      edition: ADVANCED

      # Classic compute is mandatory, not a preference. MSK is reachable only
      # from the CIDRs in kafka_client_cidrs, and classic egresses through the
      # workspace VPC NAT Gateway's Elastic IP -- one stable /32. Serverless
      # egresses from Databricks-managed ranges that rotate monthly.
      serverless: false
      clusters:
        - label: default
          node_type_id: m5d.large
          num_workers: 1

      # Triggered, not continuous: nothing downstream consumes Silver until
      # stage 2b, so an always-on cluster would burn money to feed nobody.
      continuous: false
      channel: CURRENT

      configuration:
        fdai.secret_scope: fdai
        fdai.kafka_topic: md.trades.v1
        fdai.starting_offsets: earliest
        fdai.max_offsets_per_trigger: "1000000"

      libraries:
        - glob:
            include: ../lakehouse/pipelines/trades.py
```

- [ ] **Step 5: Run the static test to verify it passes**

Run: `make lakehouse-test`
Expected: 34 passed

- [ ] **Step 6: Add the deployment targets**

Append to `Makefile`:

```makefile
.PHONY: pipeline-validate pipeline-deploy pipeline-run pipeline-refresh-bronze pipeline-status

DB_PROFILE ?= tw
DB_TARGET  ?= dev

pipeline-validate:
	databricks bundle validate -t $(DB_TARGET) --profile $(DB_PROFILE)

pipeline-deploy:
	databricks bundle deploy -t $(DB_TARGET) --profile $(DB_PROFILE)

# Code changes take effect only after a deploy, so never run without one.
pipeline-run: pipeline-deploy
	databricks bundle run trades_bronze_silver -t $(DB_TARGET) --profile $(DB_PROFILE)

# THE ONLY SANCTIONED RECOVERY after the weekly AWS sandbox wipe.
#
# The wipe destroys MSK, so Bronze's Kafka checkpoint references offsets on a
# topic that no longer exists. A whole-pipeline full refresh would fix that and
# ALSO full-refresh silver_trades -- destroying accumulated history whose source
# data is already gone. That is unrecoverable data loss.
#
# Refreshing only Bronze is safe: silver_trades is a keyed upsert, so replaying
# Bronze re-upserts the same (venue, trade_id) keys and converges.
#
# There is deliberately no target for a full-pipeline refresh.
pipeline-refresh-bronze: pipeline-deploy
	databricks bundle run trades_bronze_silver -t $(DB_TARGET) --profile $(DB_PROFILE) \
	  --full-refresh bronze_trades_stream
```

- [ ] **Step 7: Validate the bundle against the real workspace**

Run: `make pipeline-validate`
Expected: `Validation OK!` (a warning about the `dev` target prefix is normal). If it reports the glob matched no files, correct the `include` path in `resources/trades.pipeline.yml` — it is relative to the resource file, not the bundle root.

- [ ] **Step 8: Commit**

```bash
git add databricks.yml resources Makefile tests/lakehouse/test_bundle.py
git commit -m "feat(lakehouse): add asset bundle and pipeline deployment targets"
```

---

### Task 7: The immutability tripwire

The design's central correctness claim is that SCD Type 1 is lossless because a trade is immutable. That claim needs a test that can fail.

**Files:**
- Create: `lakehouse/trades/checks.py`
- Create: `tests/lakehouse/test_immutability.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `lakehouse.trades.checks.IMMUTABILITY_SQL: str` — a query returning one row per violating key, to run against `fdai.market.silver_trades`.
  - `lakehouse.trades.checks.find_immutability_violations(df: DataFrame) -> DataFrame` — the same check as a DataFrame op, for local tests.

- [ ] **Step 1: Write the failing tripwire tests**

Create `tests/lakehouse/test_immutability.py`:

```python
"""Guards the claim that makes SCD Type 1 safe (design §2.3 / parent §4.2).

If a trade's facts can change under a fixed (venue, trade_id), then a keyed
upsert silently discards one version and the design must move to SCD Type 2.
This test is the tripwire for that.
"""

from __future__ import annotations

from decimal import Decimal

from lakehouse.trades.checks import IMMUTABILITY_SQL, find_immutability_violations

_SCHEMA = "venue string, trade_id string, event_ts_us long, price decimal(38,18), size decimal(38,18)"


def test_identical_duplicates_are_not_violations(spark):
    # The stream copy and the archive copy of one trade: same facts, different
    # provenance. This must be accepted, or backfill would look like corruption.
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
    ]
    df = spark.createDataFrame(rows, _SCHEMA)
    assert find_immutability_violations(df).count() == 0


def test_differing_price_for_one_key_is_a_violation(spark):
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_000, Decimal("999.0"), Decimal("0.25")),
    ]
    df = spark.createDataFrame(rows, _SCHEMA)
    violations = find_immutability_violations(df).collect()
    assert len(violations) == 1
    assert violations[0]["trade_id"] == "1"


def test_differing_timestamp_for_one_key_is_a_violation(spark):
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_001, Decimal("100.5"), Decimal("0.25")),
    ]
    df = spark.createDataFrame(rows, _SCHEMA)
    assert find_immutability_violations(df).count() == 1


def test_same_trade_id_on_different_venues_is_not_a_violation(spark):
    # trade_id is only unique per venue, which is why the key is a pair.
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("coinbase", "1", 1_700_000_000_000_009, Decimal("101.0"), Decimal("0.30")),
    ]
    df = spark.createDataFrame(rows, _SCHEMA)
    assert find_immutability_violations(df).count() == 0


def test_sql_form_targets_the_silver_table():
    assert "fdai.market.silver_trades" in IMMUTABILITY_SQL
    assert "GROUP BY" in IMMUTABILITY_SQL.upper()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make lakehouse-test`
Expected: FAIL with `ModuleNotFoundError: No module named 'lakehouse.trades.checks'`

- [ ] **Step 3: Implement the check**

Create `lakehouse/trades/checks.py`:

```python
"""Data-quality checks that assert design invariants rather than data ranges."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Run against the deployed table. Any row returned means SCD Type 1 has become
# lossy and the design must move to SCD Type 2 -- see the design doc §2.3.
IMMUTABILITY_SQL = """
SELECT venue,
       trade_id,
       COUNT(DISTINCT event_ts_us) AS distinct_event_ts,
       COUNT(DISTINCT price)       AS distinct_price,
       COUNT(DISTINCT size)        AS distinct_size
FROM fdai.market.silver_trades
GROUP BY venue, trade_id
HAVING distinct_event_ts > 1 OR distinct_price > 1 OR distinct_size > 1
"""


def find_immutability_violations(df: DataFrame) -> DataFrame:
    """One row per (venue, trade_id) whose immutable facts disagree.

    Only event_ts_us, price and size are compared. `source` and `is_backfill`
    legitimately differ between the stream and archive copies of a trade, so
    including them would flag correct backfill as corruption.
    """
    return (
        df.groupBy("venue", "trade_id")
        .agg(
            F.countDistinct("event_ts_us").alias("distinct_event_ts"),
            F.countDistinct("price").alias("distinct_price"),
            F.countDistinct("size").alias("distinct_size"),
        )
        .where(
            (F.col("distinct_event_ts") > 1)
            | (F.col("distinct_price") > 1)
            | (F.col("distinct_size") > 1)
        )
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make lakehouse-test`
Expected: 39 passed

- [ ] **Step 5: Run the whole suite and the linters**

Run: `make lakehouse-test && uv run ruff check . && uv run ruff format --check . && uv run mypy ingest devlab lakehouse`
Expected: all clean. Fix any mypy complaint by adding precise types — do not add `# type: ignore` without a comment saying why.

If mypy cannot resolve `pyspark`, add to `pyproject.toml` under `[tool.mypy]` a per-module override rather than weakening global strictness:

```toml
[[tool.mypy.overrides]]
module = ["pyspark.*"]
ignore_missing_imports = true
```

- [ ] **Step 6: Commit**

```bash
git add lakehouse/trades/checks.py tests/lakehouse/test_immutability.py pyproject.toml
git commit -m "test(lakehouse): add the SCD Type 1 immutability tripwire"
```

---

### Task 8: Reproduction runbook and status docs

The user explicitly asked for a from-scratch ad-hoc reproduction guide.

**Files:**
- Create: `docs/RUNBOOK_STAGE_2A.md`
- Modify: `docs/SETUP.md:15-20` (the status table)

**Interfaces:** none — documentation only.

- [ ] **Step 1: Write the runbook**

Create `docs/RUNBOOK_STAGE_2A.md` covering, in order, with copy-pasteable commands:

1. **What exists after stage 2a** — the three tables, and the fact that nothing has read from Kafka yet.
2. **Prerequisites** — `uv`, Databricks CLI >= v0.292.0 (this repo used v1.11.0), `brew install openjdk@17` (needed only for local tests; note `java` stays off PATH and the Makefile sets `JAVA_HOME`).
3. **Run the local test suite from a clean clone** — `uv sync --group lakehouse`, then `make lakehouse-test`. Note the first run downloads pyspark (~300 MB) and the `spark-avro_2.12` jar into `~/.ivy2`, and that every later run is offline.
4. **Recreate the Databricks objects from nothing** — the exact commands, marked idempotent:

   ```bash
   databricks catalogs create fdai --comment "finance-data-ai lakehouse" --profile tw
   databricks schemas create market fdai --profile tw
   databricks secrets create-scope fdai --profile tw   # or let scripts/bootstrap.sh do it
   ```

   Note that `make up` already publishes `kafka_bootstrap` / `kafka_username` / `kafka_password` into that scope, and that it uses the CLI's *default* profile, so `DEFAULT` must point at the intended workspace.
5. **Validate and deploy** — `make pipeline-validate`, then `make pipeline-deploy`.
6. **What still blocks a live run** — three ordered items, flagged as separate infra work: move MSK to `ap-southeast-2`; discover the workspace NAT egress IP by running a throwaway notebook on a classic cluster that calls an IP-echo service; put that `/32` into `infra/envs/dev/terraform.tfvars` as `kafka_client_cidrs` and re-run `make up`.
7. **After the weekly sandbox wipe** — use `make pipeline-refresh-bronze` and nothing else. State plainly that a whole-pipeline full refresh destroys `silver_trades` history whose source data no longer exists, and explain why refreshing Bronze alone converges.
8. **Verifying it actually worked** — the three queries to run once data flows: row count by `source`, quarantine rate (`silver_trades_quarantine` over `bronze_trades_stream`, target < 0.1%), and `IMMUTABILITY_SQL` from `lakehouse/trades/checks.py` expecting zero rows.
9. **Teardown** — `databricks bundle destroy -t dev --profile tw`, and note that dropping the catalog is deliberately manual.

- [ ] **Step 2: Update the status table in SETUP.md**

Replace the Stage 2/3 row at `docs/SETUP.md:19` so it no longer claims no code exists:

```markdown
| Stage 2a — Bronze + Silver DLT pipeline | **Implemented, not yet run live** | `lakehouse/`, `resources/`, `databricks.yml` — blocked on the Databricks NAT EIP allowlist ([`docs/RUNBOOK_STAGE_2A.md`](RUNBOOK_STAGE_2A.md) §6) |
| Stage 2b/3 — Gold, backfill, semantic layer | **Designed, not implemented** | spec only |
```

- [ ] **Step 3: Verify the runbook's commands are accurate**

Re-read the runbook against the actual `Makefile` targets and confirm every command exists and every path resolves. A runbook with one wrong command is worse than none, because it gets trusted.

- [ ] **Step 4: Commit**

```bash
git add docs/RUNBOOK_STAGE_2A.md docs/SETUP.md
git commit -m "docs: add stage 2a reproduction runbook and update status table"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §1 scope, flow named `cdc_trades_stream` | 5 |
| §2.1 quarantine as an explicit branch | 3, 4, 5 |
| §2.2 `ADVANCED` edition | 6 |
| §2.3 Silver excludes Kafka metadata | 1 (constants), 4 (projection), 5 (`except_column_list`) |
| §4 Bronze contract, raw bytes retained | 2 |
| §4.1 `from_avro` PERMISSIVE | 2 |
| §4.2 Kafka read options, `kafkashaded.` JAAS | 5 |
| §5 all seven quarantine reasons, ms/µs tripwire | 3 |
| §6 Silver contract, DECIMAL(38,18), `event_ts` | 4 |
| §6.1 CDC config + CDF at creation | 5 |
| §7 code layout, the dp-import rule | 1, 2, 5 |
| §8 deployment, classic `m5d.large` | 6 |
| §9 post-wipe selective refresh | 6 (Makefile), 8 (runbook) |
| §10 testing matrix | 1-7 |
| §11.1 blocking infra work | 8 (documented, out of scope) |

**Gap found and closed:** the spec's §10 lists `databricks bundle validate` in CI, but this repo has no CI workflow, so Task 6 wires it to `make pipeline-validate` instead of inventing a pipeline. The on-platform convergence check from §10 is intentionally *not* a task — it needs a reachable broker, which §11.1 puts out of scope; Task 8 §8 records the queries to run once that unblocks.

**Placeholder scan:** no TBD/TODO. Tasks 3 Step 5 and 5 Step 5 deliberately remove drafting scaffolding rather than leaving it.

**Type consistency:** `decode_kafka_trades` → `classify_trades` → `valid_trades` / `quarantined_trades` chain consistently on `DataFrame`. `QUARANTINE_REASON` is defined once in `schema.py` and imported everywhere. `BRONZE_AUDIT_COLUMNS` is the single source for both the projection and `except_column_list`, and Task 5's contract test asserts they agree.
