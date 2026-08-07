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
