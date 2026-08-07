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
        if not isinstance(result, dict):
            raise TypeError(f"expected a record (dict), got {type(result)!r}")
        return result


@lru_cache(maxsize=8)
def trade_codec(version: int = TRADE_SCHEMA_VERSION) -> AvroCodec:
    path = SCHEMA_DIR / f"trade.v{version}.avsc"
    if not path.exists():
        raise FileNotFoundError(f"no trade schema for version {version}: {path}")
    return AvroCodec(path)
