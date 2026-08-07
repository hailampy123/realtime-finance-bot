import dataclasses

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Side, Source, Trade
from ingest.core.producer import TradeProducer, build_config

TRADE = Trade(
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


class FakeKafka:
    def __init__(self, config):
        self.config = config
        self.produced: list[dict] = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": dict(headers or [])}
        )

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def make_producer() -> tuple[TradeProducer, FakeKafka]:
    holder: list[FakeKafka] = []

    def factory(config):
        holder.append(FakeKafka(config))
        return holder[-1]

    producer = TradeProducer("broker:9096", producer_factory=factory)
    return producer, holder[0]


def test_config_enables_idempotence_and_full_acks():
    config = build_config("broker:9096", None, None)
    assert config["enable.idempotence"] is True
    assert str(config["acks"]) == "all"


def test_sasl_is_configured_only_when_credentials_are_supplied():
    plain = build_config("broker:9096", None, None)
    assert "sasl.mechanisms" not in plain

    secured = build_config("broker:9096", "user", "pass")
    assert secured["security.protocol"] == "SASL_SSL"
    assert secured["sasl.mechanisms"] == "SCRAM-SHA-512"
    assert secured["sasl.username"] == "user"


def test_produce_uses_venue_symbol_key():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    assert kafka.produced[0]["key"] == b"binance|BTCUSDT"


def test_produce_writes_decodable_avro():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    decoded = trade_codec().decode(kafka.produced[0]["value"])
    assert decoded["trade_id"] == "12345"
    assert decoded["price"] == "43210.55"


def test_schema_version_travels_as_a_header():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", TRADE)
    headers = kafka.produced[0]["headers"]
    assert headers["schema_version"] == str(TRADE_SCHEMA_VERSION).encode()
    assert headers["is_backfill"] == b"false"


def test_backfill_flag_reaches_the_header():
    producer, kafka = make_producer()
    producer.produce("md.trades.v1", dataclasses.replace(TRADE, is_backfill=True))
    assert kafka.produced[0]["headers"]["is_backfill"] == b"true"
