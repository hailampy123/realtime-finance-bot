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
