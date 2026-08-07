import asyncio
import json
from pathlib import Path

import pytest

from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Side, Source, Trade
from ingest.core.queue import BoundedTopicQueue, DropPolicy
from ingest.runner import IngestRunner

FIXTURE = Path(__file__).parent / "fixtures" / "binance_aggtrade.json"
COINBASE_FIXTURE = Path(__file__).parent / "fixtures" / "coinbase_market_trades.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT}
"""

COINBASE_UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {coinbase: BTC-USD}
"""


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    def produce(self, topic, trade):
        self.sent.append((topic, trade))

    def poll(self, timeout=0.0):
        return 0

    def flush(self, timeout=10.0):
        return 0


@pytest.fixture
def wiring(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(UNIVERSE)
    connector = BinanceConnector(InstrumentMap.from_yaml(path))
    producer = RecordingProducer()
    queue = BoundedTopicQueue(maxsize=64, policy=DropPolicy.BLOCK)
    runner = IngestRunner(connector, producer, SequenceTracker(), queue)
    return runner, producer, queue


@pytest.fixture
def coinbase_wiring(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(COINBASE_UNIVERSE)
    connector = CoinbaseConnector(InstrumentMap.from_yaml(path))
    producer = RecordingProducer()
    queue = BoundedTopicQueue(maxsize=64, policy=DropPolicy.BLOCK)
    runner = IngestRunner(connector, producer, SequenceTracker(), queue)
    return runner, producer, queue


async def test_message_lands_on_the_queue(wiring):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())
    assert queue.qsize() == 1


async def test_drain_publishes_queued_trades(wiring):
    runner, producer, _ = wiring
    await runner.handle_message(FIXTURE.read_text())
    stop = asyncio.Event()
    task = asyncio.create_task(runner.drain(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert producer.sent[0][0] == "md.trades.v1"
    assert producer.sent[0][1].trade_id == "987654321"


async def test_a_sequence_jump_triggers_repair_and_enqueues_the_recovered_trades(
    wiring, monkeypatch
):
    runner, _, queue = wiring
    first = json.loads(FIXTURE.read_text())
    await runner.handle_message(json.dumps(first))

    async def fake_repair(gap):
        from ingest.core.models import Side, Trade

        return [
            Trade(
                venue="binance",
                venue_symbol="BTCUSDT",
                instrument_id="BTC-USD",
                trade_id=str(gap.last_seen + 1),
                event_ts_us=1,
                ingest_ts_us=1,
                price="1",
                size="1",
                side=Side.BUY,
                sequence=gap.last_seen + 1,
                is_backfill=True,
                source=Source.REST_REPAIR,
            )
        ]

    monkeypatch.setattr(runner.connector, "repair", fake_repair)

    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 987654325
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)

    assert queue.qsize() == 3  # original + repaired + jumped


async def test_repair_failure_does_not_kill_the_stream(wiring, monkeypatch, caplog):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())

    async def failing_repair(gap):
        raise RuntimeError("venue is down")

    monkeypatch.setattr(runner.connector, "repair", failing_repair)

    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 987654325
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)

    assert queue.qsize() == 2  # the live trades still made it through


async def test_drain_flushes_everything_already_queued_even_if_stop_is_already_set(wiring):
    """Reproduces the exact shutdown race: stop fires while trades are still queued."""
    runner, producer, queue = wiring
    for i in range(5):
        await queue.put(f"trade-{i}")
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(runner.drain(stop), timeout=1.0)
    assert [t for _, t in producer.sent] == [f"trade-{i}" for i in range(5)]
    assert queue.qsize() == 0


async def test_reconnect_preserves_watermark_for_persistent_sequence_venues_so_gap_still_repairs(
    wiring, monkeypatch
):
    """Binance's aggTrade id is persistent across reconnects, so a reconnect must
    NOT forget the watermark — a genuine gap that occurred during the outage has
    to still be detected and repaired, not silently treated as a fresh baseline.
    """
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())

    async def fake_repair(gap):
        return [
            Trade(
                venue="binance",
                venue_symbol="BTCUSDT",
                instrument_id="BTC-USD",
                trade_id=str(gap.last_seen + 1),
                event_ts_us=1,
                ingest_ts_us=1,
                price="1",
                size="1",
                side=Side.BUY,
                sequence=gap.last_seen + 1,
                is_backfill=True,
                source=Source.REST_REPAIR,
            )
        ]

    monkeypatch.setattr(runner.connector, "repair", fake_repair)

    await runner.handle_reconnect()

    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 999999999
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)

    assert queue.qsize() == 3  # original + repaired + jumped: the gap was still caught


async def test_reconnect_resets_the_watermark_for_connection_scoped_sequence_venues(
    coinbase_wiring,
):
    """Coinbase's sequence_num is connection-scoped, so a reconnect legitimately
    restarts it — the old, reset-by-default behaviour must still hold here.
    """
    runner, _, queue = coinbase_wiring
    await runner.handle_message(COINBASE_FIXTURE.read_text())
    await runner.handle_reconnect()
    jumped = json.loads(COINBASE_FIXTURE.read_text())
    jumped["sequence_num"] = 999999999
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)
    assert queue.qsize() == 2  # no repair records injected


async def test_in_flight_repair_tasks_are_awaited_before_drain_observes_an_empty_queue(wiring):
    """Reproduces the shutdown race: a repair resolves and enqueues a trade after
    stop is set. If shutdown only awaited drain_task, drain() could already have
    seen an empty queue and returned before the repair's trade was put, orphaning
    it. Awaiting in-flight repair tasks first (as run()'s finally block now does)
    guarantees drain() still sees the trade.
    """
    runner, producer, queue = wiring
    trade = Trade(
        venue="binance",
        venue_symbol="BTCUSDT",
        instrument_id="BTC-USD",
        trade_id="1",
        event_ts_us=1,
        ingest_ts_us=1,
        price="1",
        size="1",
        side=Side.BUY,
        sequence=1,
        is_backfill=True,
        source=Source.REST_REPAIR,
    )

    async def slow_repair_task() -> None:
        await asyncio.sleep(0.05)
        await runner.queue.put(trade)

    task = asyncio.create_task(slow_repair_task())
    runner._repair_tasks.add(task)
    task.add_done_callback(runner._repair_tasks.discard)

    stop = asyncio.Event()
    stop.set()  # shutdown already requested; queue is currently empty

    # Mirror IngestRunner.run()'s finally block ordering: repairs before drain.
    await asyncio.gather(*list(runner._repair_tasks), return_exceptions=True)
    await runner.drain(stop)

    assert [t.trade_id for _, t in producer.sent] == ["1"]
    assert queue.qsize() == 0
