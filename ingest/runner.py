from __future__ import annotations

import asyncio
import contextlib

import structlog

from ingest.connectors.base import Connector
from ingest.core.gaps import SequenceTracker
from ingest.core.models import Trade
from ingest.core.queue import BoundedTopicQueue
from ingest.core.sinks import Sink
from ingest.core.ws import ResilientWebSocket

log = structlog.get_logger(__name__)


class IngestRunner:
    """Glues one connector to one Kafka topic.

    Parsing populates a bounded queue; a separate drain task publishes. The
    split matters: publishing must never block frame consumption, and the
    queue's policy (BLOCK for trades) is what turns saturation into a
    detectable, repairable gap rather than silent loss.
    """

    def __init__(
        self,
        connector: Connector,
        producer: Sink,
        tracker: SequenceTracker,
        queue: BoundedTopicQueue[Trade],
        topic: str = "md.trades.v1",
    ) -> None:
        self.connector = connector
        self.producer = producer
        self.tracker = tracker
        self.queue = queue
        self.topic = topic
        self._repair_tasks: set[asyncio.Task[None]] = set()

    async def handle_message(self, raw: str) -> None:
        for trade in self.connector.parse(raw):
            sequence_symbol = getattr(self.connector, "sequence_symbol", trade.venue_symbol)
            gap = self.tracker.observe(trade.venue, sequence_symbol, trade.sequence)
            if gap is not None:
                task = asyncio.create_task(self._repair(gap))
                self._repair_tasks.add(task)
                task.add_done_callback(self._repair_tasks.discard)
            await self.queue.put(trade)

    async def _repair(self, gap: object) -> None:
        try:
            for trade in await self.connector.repair(gap):  # type: ignore[arg-type]
                await self.queue.put(trade)
        except Exception as exc:  # a failed repair must not stop the stream
            log.error("gap_repair_failed", error=str(exc))

    async def handle_reconnect(self) -> None:
        """A fresh connection restarts a connection-scoped sequence; venues whose
        sequence id is persistent across reconnects (e.g. Binance's aggTrade id)
        must keep their watermark so a real gap during the outage is still detected.
        """
        if getattr(self.connector, "resets_sequence_on_reconnect", True):
            self.tracker = SequenceTracker()
            log.info("sequence_watermarks_reset", venue=self.connector.venue)
        else:
            log.info("sequence_watermarks_preserved", venue=self.connector.venue)

    async def drain(self, stop: asyncio.Event) -> None:
        while not stop.is_set() or self.queue.qsize() > 0:
            try:
                trade = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                self.producer.poll(0)
                continue
            self.producer.produce(self.topic, trade)
            self.producer.poll(0)
        self.producer.flush(10.0)

    async def run(self, stop: asyncio.Event, symbols: list[str]) -> None:
        socket = ResilientWebSocket(
            self.connector.stream_url(symbols),
            self.connector.subscribe_payloads(symbols),
            self.handle_message,
            self.handle_reconnect,
        )
        drain_task = asyncio.create_task(self.drain(stop))
        try:
            await socket.run(stop)
        finally:
            stop.set()
            if self._repair_tasks:
                await asyncio.gather(*list(self._repair_tasks), return_exceptions=True)
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
