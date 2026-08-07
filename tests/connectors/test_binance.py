import json
from pathlib import Path

import pytest

from ingest.connectors.binance import BinanceConnector
from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source

FIXTURE = Path(__file__).parent.parent / "fixtures" / "binance_aggtrade.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT}
  - id: ETH-USD
    asset_class: crypto
    venues: {binance: ETHUSDT}
"""


@pytest.fixture
def connector(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    return BinanceConnector(InstrumentMap.from_yaml(path))


def test_stream_url_lowercases_and_joins_symbols(connector):
    url = connector.stream_url(["BTCUSDT", "ETHUSDT"])
    assert url == ("wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade")


def test_binance_needs_no_subscribe_frame(connector):
    assert connector.subscribe_payloads(["BTCUSDT"]) == []


def test_parses_the_recorded_frame(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.venue == "binance"
    assert trade.venue_symbol == "BTCUSDT"
    assert trade.instrument_id == "BTC-USD"
    assert trade.trade_id == "987654321"
    assert trade.sequence == 987654321
    assert trade.price == "43210.55000000"
    assert trade.size == "0.00120000"
    assert trade.source is Source.STREAM
    assert trade.is_backfill is False


def test_event_time_converts_milliseconds_to_microseconds(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.event_ts_us == 1_700_000_000_100_000


def test_buyer_maker_true_means_the_seller_was_the_aggressor(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.side is Side.SELL


def test_buyer_maker_false_means_the_buyer_was_the_aggressor(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["data"]["m"] = False
    (trade,) = connector.parse(json.dumps(frame))
    assert trade.side is Side.BUY


def test_non_trade_frames_are_ignored(connector):
    assert connector.parse(json.dumps({"result": None, "id": 1})) == []


def test_unmapped_symbol_is_skipped_not_crashed(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["data"]["s"] = "DOGEUSDT"
    assert connector.parse(json.dumps(frame)) == []


async def test_repair_fetches_the_missing_id_range(connector, monkeypatch):
    captured: dict = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "a": 101,
                    "p": "1.5",
                    "q": "2.0",
                    "f": 1,
                    "l": 1,
                    "T": 1_700_000_000_000,
                    "m": False,
                },
                {
                    "a": 102,
                    "p": "1.6",
                    "q": "2.1",
                    "f": 2,
                    "l": 2,
                    "T": 1_700_000_000_001,
                    "m": True,
                },
            ]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.binance.httpx.AsyncClient", lambda **kw: FakeClient())

    gap = Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    trades = await connector.repair(gap)

    assert captured["params"]["symbol"] == "BTCUSDT"
    assert captured["params"]["fromId"] == 101
    assert [t.trade_id for t in trades] == ["101", "102"]
    assert all(t.is_backfill for t in trades)
    assert all(t.source is Source.REST_REPAIR for t in trades)


async def test_repair_does_not_return_ids_at_or_beyond_the_resume_point(connector, monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {"a": 104, "p": "1", "q": "1", "T": 1, "m": False},
                {"a": 105, "p": "1", "q": "1", "T": 1, "m": False},
                {"a": 106, "p": "1", "q": "1", "T": 1, "m": False},
            ]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.binance.httpx.AsyncClient", lambda **kw: FakeClient())
    gap = Gap(venue="binance", venue_symbol="BTCUSDT", last_seen=100, next_seen=105)
    trades = await connector.repair(gap)
    assert [t.trade_id for t in trades] == ["104"]
