import json
from pathlib import Path

import pytest

from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import Gap
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source

FIXTURE = Path(__file__).parent.parent / "fixtures" / "coinbase_market_trades.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {coinbase: BTC-USD}
"""


@pytest.fixture
def connector(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    return CoinbaseConnector(InstrumentMap.from_yaml(path))


def test_stream_url_is_the_advanced_trade_endpoint(connector):
    assert connector.stream_url(["BTC-USD"]) == "wss://advanced-trade-ws.coinbase.com"


def test_subscribe_payload_requests_market_trades(connector):
    (payload,) = connector.subscribe_payloads(["BTC-USD", "ETH-USD"])
    assert payload == {
        "type": "subscribe",
        "channel": "market_trades",
        "product_ids": ["BTC-USD", "ETH-USD"],
    }


def test_parses_the_recorded_frame(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.venue == "coinbase"
    assert trade.venue_symbol == "BTC-USD"
    assert trade.instrument_id == "BTC-USD"
    assert trade.trade_id == "555000111"
    assert trade.price == "43215.10"
    assert trade.side is Side.BUY
    assert trade.source is Source.STREAM


def test_rfc3339_time_becomes_microseconds(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.event_ts_us == 1_786_104_000_123_456


def test_envelope_sequence_is_attached_to_every_trade(connector):
    (trade,) = connector.parse(FIXTURE.read_text())
    assert trade.sequence == 42


def test_snapshot_frames_are_parsed_like_updates(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["events"][0]["type"] = "snapshot"
    assert len(connector.parse(json.dumps(frame))) == 1


def test_heartbeat_and_subscription_acks_are_ignored(connector):
    assert connector.parse(json.dumps({"channel": "subscriptions", "events": []})) == []
    assert connector.parse(json.dumps({"channel": "heartbeats", "events": []})) == []


def test_unmapped_product_is_skipped(connector):
    frame = json.loads(FIXTURE.read_text())
    frame["events"][0]["trades"][0]["product_id"] = "DOGE-USD"
    assert connector.parse(json.dumps(frame)) == []


def test_missing_side_falls_back_to_unknown_without_crashing_the_frame(connector):
    frame = json.loads(FIXTURE.read_text())
    del frame["events"][0]["trades"][0]["side"]
    (trade,) = connector.parse(json.dumps(frame))
    assert trade.side is Side.UNKNOWN


def test_a_malformed_trade_does_not_prevent_siblings_in_the_same_frame_from_parsing(connector):
    frame = json.loads(FIXTURE.read_text())
    good_trade = json.loads(json.dumps(frame["events"][0]["trades"][0]))
    good_trade["trade_id"] = "555000999"
    bad_trade = dict(frame["events"][0]["trades"][0])
    del bad_trade["side"]  # still parses under the fix above, so use a harder failure:
    del bad_trade["price"]  # required field, to prove containment, not just the fallback
    frame["events"][0]["trades"] = [bad_trade, good_trade]
    trades = connector.parse(json.dumps(frame))
    assert [t.trade_id for t in trades] == ["555000999"]


def test_sequence_key_is_connection_wide_not_per_symbol(connector):
    assert connector.sequence_symbol == "*"


async def test_repair_refetches_recent_trades_best_effort(connector, monkeypatch):
    captured: dict = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "trades": [
                    {
                        "trade_id": "555000112",
                        "product_id": "BTC-USD",
                        "price": "43216.00",
                        "size": "0.001",
                        "side": "SELL",
                        "time": "2026-08-07T12:00:01.000000Z",
                    }
                ]
            }

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            captured["url"] = url
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.coinbase.httpx.AsyncClient", lambda **kw: FakeClient())

    gap = Gap(venue="coinbase", venue_symbol="BTC-USD", last_seen=42, next_seen=45)
    trades = await connector.repair(gap)

    assert "BTC-USD" in captured["url"]
    assert [t.trade_id for t in trades] == ["555000112"]
    assert all(t.is_backfill for t in trades)
    assert all(t.source is Source.REST_REPAIR for t in trades)


async def test_a_malformed_repair_row_does_not_prevent_siblings_from_being_recovered(
    connector, monkeypatch
):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "trades": [
                    # missing "price" — _to_trade's str(row["price"]) will raise KeyError.
                    {
                        "trade_id": "555000112",
                        "product_id": "BTC-USD",
                        "size": "0.001",
                        "side": "SELL",
                        "time": "2026-08-07T12:00:01.000000Z",
                    },
                    {
                        "trade_id": "555000113",
                        "product_id": "BTC-USD",
                        "price": "43217.00",
                        "size": "0.002",
                        "side": "BUY",
                        "time": "2026-08-07T12:00:02.000000Z",
                    },
                ]
            }

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.coinbase.httpx.AsyncClient", lambda **kw: FakeClient())
    gap = Gap(venue="coinbase", venue_symbol="BTC-USD", last_seen=42, next_seen=45)
    trades = await connector.repair(gap)
    assert [t.trade_id for t in trades] == ["555000113"]


async def test_repair_of_the_wildcard_symbol_covers_every_configured_product(
    connector, monkeypatch
):
    calls: list[str] = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"trades": []}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None):
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr("ingest.connectors.coinbase.httpx.AsyncClient", lambda **kw: FakeClient())
    gap = Gap(venue="coinbase", venue_symbol="*", last_seen=42, next_seen=45)
    await connector.repair(gap)
    assert len(calls) == 1  # one product configured in this fixture universe
