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
        # Pin the driver to loopback. Left to itself Spark binds the machine's
        # LAN address, which fails the moment the laptop changes network -- and
        # showed up here as an intermittent "Error initializing SparkContext".
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Pin UTC. spark.sql.session.timeZone defaults to the JVM's zone, so
        # timestamp_micros would render event_ts in whatever zone the developer
        # happens to sit in -- shifting a financial time series by hours
        # depending on where the code ran. Databricks defaults to UTC; matching
        # it here is what makes the local suite representative. The pipeline
        # sets the same value explicitly rather than trusting the default.
        .config("spark.sql.session.timeZone", "UTC")
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
