"""Round-trips real records through a real broker.

Guarded by RUN_INTEGRATION=1 so `make test` stays fast and hermetic.
"""

import socket
import uuid

import pytest
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade
from ingest.core.producer import TradeProducer

BOOTSTRAP = "127.0.0.1:9092"


def test_every_advertised_broker_address_is_reachable_from_the_host():
    """A client may select any address returned for the advertised hostname."""
    metadata = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=15)

    for broker in metadata.brokers.values():
        addresses = socket.getaddrinfo(broker.host, broker.port, type=socket.SOCK_STREAM)
        assert addresses
        for family, socktype, proto, _, sockaddr in addresses:
            with socket.socket(family, socktype, proto) as client:
                client.settimeout(2)
                client.connect(sockaddr)


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
    assert len({m.partition() for m in messages}) == 1, (
        "ordering per instrument requires one partition"
    )


def test_different_symbols_can_spread_across_partitions(topic):
    producer = TradeProducer(BOOTSTRAP)
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"):
        for i in range(5):
            producer.produce(topic, make_trade(str(700 + i), symbol=symbol))
    producer.flush(30.0)

    messages = consume(topic, expected=20)
    assert len({m.partition() for m in messages}) >= 2
