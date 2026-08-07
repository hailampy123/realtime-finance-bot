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
