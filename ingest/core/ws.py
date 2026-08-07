"""A WebSocket connection that assumes it will be dropped, because it will be.

Three behaviours matter here and none are default:
  * exponential backoff with jitter, so a venue outage does not become a
    thundering-herd reconnect storm;
  * proactive reconnect before the venue's own connection lifetime expires
    (Binance closes at 24h) — reconnecting on our schedule beats being dropped;
  * an on_reconnect hook, which is where gap repair is triggered.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Protocol

import structlog

log = structlog.get_logger(__name__)


class Socket(Protocol):
    async def send(self, payload: str) -> None: ...
    def __aiter__(self) -> AsyncIterator[str]: ...
    async def __aenter__(self) -> Socket: ...
    async def __aexit__(self, *exc: object) -> bool: ...


ConnectFn = Callable[[str], Awaitable[Socket]]


def backoff_delays(max_backoff_s: float, jitter: Callable[[], float]) -> Iterator[float]:
    """1s, 2s, 4s ... capped, each multiplied by (1 + jitter())."""
    delay = 1.0
    while True:
        yield min(delay * (1.0 + jitter()), max_backoff_s)
        delay = min(delay * 2.0, max_backoff_s)


async def _default_connect(url: str) -> Socket:
    import websockets

    return await websockets.connect(url, ping_interval=20, ping_timeout=20)  # type: ignore[return-value]


class ResilientWebSocket:
    def __init__(
        self,
        url: str,
        subscribe: list[dict[str, Any]],
        on_message: Callable[[str], Awaitable[None]],
        on_reconnect: Callable[[], Awaitable[None]] | None = None,
        *,
        max_lifetime_s: float = 23 * 3600,
        max_backoff_s: float = 60.0,
        connect: ConnectFn | None = None,
    ) -> None:
        self.url = url
        self.subscribe = subscribe
        self.on_message = on_message
        self.on_reconnect = on_reconnect
        self.max_lifetime_s = max_lifetime_s
        self.max_backoff_s = max_backoff_s
        self._connect = connect or _default_connect
        self.last_sent: list[str] = []

    async def run(self, stop: asyncio.Event) -> None:
        delays = backoff_delays(self.max_backoff_s, lambda: random.random() * 0.3)
        first_connection = True
        while not stop.is_set():
            try:
                await self._session(stop, first_connection)
                delays = backoff_delays(self.max_backoff_s, lambda: random.random() * 0.3)
            except Exception as exc:  # any failure means reconnect
                log.warning("ws_session_failed", url=self.url, error=str(exc))
            first_connection = False
            if stop.is_set():
                return
            await asyncio.sleep(next(delays))

    async def _session(self, stop: asyncio.Event, first_connection: bool) -> None:
        socket = await self._connect(self.url)
        async with socket:
            for payload in self.subscribe:
                encoded = json.dumps(payload)
                self.last_sent.append(encoded)
                await socket.send(encoded)
            if not first_connection and self.on_reconnect is not None:
                await self.on_reconnect()
            deadline = asyncio.get_running_loop().time() + self.max_lifetime_s
            async for raw in socket:
                await self.on_message(raw)
                if stop.is_set():
                    return
                if asyncio.get_running_loop().time() >= deadline:
                    log.info("ws_proactive_reconnect", url=self.url)
                    return
