import asyncio
import json

import pytest

from ingest.core.ws import ResilientWebSocket, backoff_delays


def test_backoff_grows_exponentially_and_caps():
    delays = list(itertools_take(backoff_delays(max_backoff_s=30.0, jitter=lambda: 0.0), 8))
    assert delays[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert all(d <= 30.0 for d in delays)


def itertools_take(iterator, n):
    return [next(iterator) for _ in range(n)]


def test_backoff_applies_jitter():
    delays = itertools_take(backoff_delays(max_backoff_s=30.0, jitter=lambda: 0.5), 3)
    assert delays == [1.5, 3.0, 6.0]


class FakeSocket:
    """Yields a scripted list of frames, then raises to simulate a drop."""

    def __init__(self, frames: list[str], fail_after: bool = True) -> None:
        self.frames = frames
        self.fail_after = fail_after
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for frame in self.frames:
            yield frame
        if self.fail_after:
            raise ConnectionResetError("simulated drop")


async def test_delivers_frames_to_on_message():
    received: list[str] = []
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}', '{"a":2}'], fail_after=False)]

    async def connect(url: str):
        return sockets.pop(0)

    async def on_message(raw: str) -> None:
        received.append(raw)
        if len(received) == 2:
            stop.set()

    ws = ResilientWebSocket("wss://x", [{"sub": 1}], on_message, connect=connect)
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert received == ['{"a":1}', '{"a":2}']


async def test_sends_subscribe_payload_on_every_connection():
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}']), FakeSocket(['{"a":2}'], fail_after=False)]

    async def connect(url: str):
        return sockets.pop(0)

    seen: list[str] = []

    async def on_message(raw: str) -> None:
        seen.append(raw)
        if len(seen) == 2:
            stop.set()

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, connect=connect, max_backoff_s=0.01
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert json.loads(sockets_sent(ws)[0]) == {"sub": 1}


def sockets_sent(ws):
    return ws.last_sent


async def test_reconnect_hook_fires_after_a_drop():
    stop = asyncio.Event()
    sockets = [FakeSocket(['{"a":1}']), FakeSocket(['{"a":2}'], fail_after=False)]
    reconnects: list[int] = []

    async def connect(url: str):
        return sockets.pop(0)

    seen: list[str] = []

    async def on_message(raw: str) -> None:
        seen.append(raw)
        if len(seen) == 2:
            stop.set()

    async def on_reconnect() -> None:
        reconnects.append(1)

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, on_reconnect, connect=connect, max_backoff_s=0.01
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert len(reconnects) == 1, "the hook must fire on the reconnect, not the first connect"


async def test_stop_event_ends_the_loop():
    stop = asyncio.Event()
    stop.set()

    async def connect(url: str):
        pytest.fail("must not connect once stop is already set")

    ws = ResilientWebSocket("wss://x", [], lambda raw: asyncio.sleep(0), connect=connect)
    await asyncio.wait_for(ws.run(stop), timeout=1.0)


class SilentSocket:
    """Connects and subscribes, then never delivers a frame."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        await asyncio.Event().wait()  # never fires
        yield ""  # pragma: no cover


async def test_no_reconnect_hook_when_the_first_dial_fails():
    """A failed dial must not make the next connection look like a reconnect."""
    stop = asyncio.Event()
    reconnects: list[int] = []
    attempts = {"n": 0}

    async def connect(url: str):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionRefusedError("first dial fails")
        return FakeSocket(['{"a":1}'], fail_after=False)

    async def on_message(raw: str) -> None:
        stop.set()

    async def on_reconnect() -> None:
        reconnects.append(1)

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, on_reconnect, connect=connect, max_backoff_s=0.01
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert attempts["n"] == 2
    assert reconnects == [], "on_reconnect fired on the first successful connection"


async def test_stop_is_observed_while_the_socket_is_silent():
    """Shutdown must not wait for a frame that may never arrive."""
    stop = asyncio.Event()

    async def connect(url: str):
        return SilentSocket()

    async def on_message(raw: str) -> None:  # pragma: no cover - never called
        raise AssertionError("silent socket delivered a frame")

    ws = ResilientWebSocket("wss://x", [{"sub": 1}], on_message, connect=connect)
    task = asyncio.create_task(ws.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)


async def test_proactive_reconnect_fires_on_a_silent_socket():
    """The lifetime deadline must expire even when no frames arrive."""
    stop = asyncio.Event()
    connects = {"n": 0}

    async def connect(url: str):
        connects["n"] += 1
        if connects["n"] >= 2:
            stop.set()
        return SilentSocket()

    async def on_message(raw: str) -> None:  # pragma: no cover - never called
        raise AssertionError("silent socket delivered a frame")

    ws = ResilientWebSocket(
        "wss://x", [{"sub": 1}], on_message, connect=connect, max_lifetime_s=0.05
    )
    await asyncio.wait_for(ws.run(stop), timeout=2.0)
    assert connects["n"] >= 2, "lifetime deadline did not trigger a reconnect"
