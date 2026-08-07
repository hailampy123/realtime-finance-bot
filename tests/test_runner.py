import asyncio
import json
from pathlib import Path

import pytest

from ingest.connectors.binance import BinanceConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.models import Source
from ingest.core.queue import BoundedTopicQueue, DropPolicy
from ingest.runner import IngestRunner

FIXTURE = Path(__file__).parent / "fixtures" / "binance_aggtrade.json"

UNIVERSE = """
instruments:
  - id: BTC-USD
    asset_class: crypto
    venues: {binance: BTCUSDT}
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


async def test_reconnect_resets_the_watermark_so_a_new_stream_is_not_a_gap(wiring):
    runner, _, queue = wiring
    await runner.handle_message(FIXTURE.read_text())
    await runner.handle_reconnect()
    jumped = json.loads(FIXTURE.read_text())
    jumped["data"]["a"] = 999999999
    await runner.handle_message(json.dumps(jumped))
    await asyncio.sleep(0.05)
    assert queue.qsize() == 2  # no repair records injected
