import pytest

from ingest.core.codec import trade_codec

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
