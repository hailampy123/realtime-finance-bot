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
