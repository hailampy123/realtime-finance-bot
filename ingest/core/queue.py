"""Backpressure policy, stated per topic rather than left to emerge.

Depth updates are recoverable from the next snapshot, so they may be dropped.
Trades are not recoverable from anything cheaper than a REST call, so the
producer blocks, takes the gap, and repairs it. Silent trade loss is the one
failure this system must not have.
"""

from __future__ import annotations

import asyncio
from enum import StrEnum

import structlog

log = structlog.get_logger(__name__)


class DropPolicy(StrEnum):
    BLOCK = "BLOCK"
    DROP_OLDEST = "DROP_OLDEST"


TOPIC_POLICIES: dict[str, DropPolicy] = {
    "md.trades.v1": DropPolicy.BLOCK,
    "md.bars.v1": DropPolicy.BLOCK,
    "md.book.top.v1": DropPolicy.DROP_OLDEST,
    "md.book.depth.v1": DropPolicy.DROP_OLDEST,
    "news.articles.v1": DropPolicy.BLOCK,
    "ops.metrics.v1": DropPolicy.DROP_OLDEST,
}


class BoundedTopicQueue[T]:
    def __init__(self, maxsize: int, policy: DropPolicy) -> None:
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self._policy = policy
        self._dropped = 0

    @property
    def dropped(self) -> int:
        return self._dropped

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, item: T) -> None:
        if self._policy is DropPolicy.BLOCK:
            await self._queue.put(item)
            return
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()
                    self._dropped += 1
                    log.warning("queue_dropped_oldest", total_dropped=self._dropped)
                except asyncio.QueueEmpty:  # pragma: no cover - racy shrink
                    continue

    async def get(self) -> T:
        return await self._queue.get()
