from __future__ import annotations

import json

import fastavro
import pytest

from awsnative.encode import encode_trade, trade_to_dict
from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade


def make_trade(**overrides: object) -> Trade:
    base = dict(
        venue="binance",
        venue_symbol="BTCUSDT",
        instrument_id="BTC-USD",
        trade_id="12345",
        event_ts_us=1_754_000_000_000_000,
        ingest_ts_us=1_754_000_000_100_000,
        price="61234.56",
        size="0.0123",
        side=Side.BUY,
        sequence=987,
        is_backfill=False,
        source=Source.STREAM,
    )
    base.update(overrides)
    return Trade(**base)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "trade",
    [
        make_trade(),
        make_trade(side=Side.SELL, sequence=None),
        make_trade(source=Source.ARCHIVE, is_backfill=True),
        make_trade(side=Side.UNKNOWN, source=Source.REST_REPAIR),
    ],
    ids=["plain", "no-sequence", "archive", "unknown-side"],
)
def test_json_payload_validates_against_the_avro_schema(trade: Trade) -> None:
    """The drift tripwire.

    trade.v1.avsc stays the single source of truth for the record shape. If a
    field is added to the schema and not to the encoder (or vice versa), this
    fails in CI rather than as a Firehose delivery error at 3am.
    """
    payload = trade_to_dict(trade)
    schema_fields = dict(payload)
    schema_fields.pop("schema_version")  # transport metadata, not part of the record

    assert fastavro.validate(schema_fields, trade_codec().schema, raise_errors=True)


def test_encoder_and_avro_schema_agree_on_field_names() -> None:
    """Catches a renamed field, which validate() alone would let through if the
    old name were simply absent and nullable."""
    payload = trade_to_dict(make_trade())
    encoder_fields = set(payload) - {"schema_version"}
    avro_fields = {f["name"] for f in trade_codec().schema["fields"]}

    assert encoder_fields == avro_fields


def test_schema_version_is_carried_in_the_body() -> None:
    """Kinesis has no headers, so the version the Kafka path puts in a header
    has to travel inside the record."""
    assert trade_to_dict(make_trade())["schema_version"] == TRADE_SCHEMA_VERSION


def test_encode_trade_is_compact_utf8_json_without_a_trailing_newline() -> None:
    """Firehose's OpenX JSON deserializer reads one JSON document per record.
    A trailing newline or pretty-printing is wasted bytes on a per-GB bill."""
    raw = encode_trade(make_trade())

    assert isinstance(raw, bytes)
    assert not raw.endswith(b"\n")
    assert b", " not in raw and b": " not in raw
    assert json.loads(raw.decode())["venue"] == "binance"


def test_enums_encode_as_their_string_values() -> None:
    payload = trade_to_dict(make_trade(side=Side.SELL, source=Source.ARCHIVE))

    assert payload["side"] == "SELL"
    assert payload["source"] == "ARCHIVE"
