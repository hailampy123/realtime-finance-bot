from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from devlab import config, stream
from tests.devlab.conftest import record


class FakeMessage:
    def __init__(
        self,
        partition: int = 3,
        offset: int = 42,
        key: bytes | None = b"binance|BTCUSDT",
    ):
        self._partition = partition
        self._offset = offset
        self._key = key

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return b"encoded"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_700_000_000_123)

    def key(self) -> bytes | None:
        return self._key


class StubCodec:
    """Returns a fresh dict per call — tail() mutates what it decodes."""

    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or record()

    def decode(self, data: bytes) -> dict[str, Any]:
        return dict(self._payload)


@contextmanager
def _fake_consumer(events: dict[str, Any], messages: list[Any]):
    queue = list(messages)

    class FakeConsumer:
        def poll(self, timeout: float) -> Any:
            events["polls"] = events.get("polls", 0) + 1
            return queue.pop(0) if queue else None

    try:
        yield FakeConsumer()
    finally:
        events["closed"] = True


def patch_stream(monkeypatch, events: dict[str, Any], messages: list[Any]) -> None:
    monkeypatch.setattr(stream, "consumer", lambda *a, **k: _fake_consumer(events, messages))
    monkeypatch.setattr(stream, "trade_codec", StubCodec)


def test_a_tail_with_no_bound_is_refused():
    # The point of the guard: a notebook cell polling a quiet topic forever
    # looks identical to a broken broker.
    with pytest.raises(ValueError, match="unbounded"):
        next(stream.tail(config.local(), limit=None, seconds=None))


def test_a_nonpositive_limit_is_refused():
    with pytest.raises(ValueError, match="limit must be positive"):
        next(stream.tail(config.local(), limit=0))


def test_the_limit_stops_the_tail(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage() for _ in range(10)])
    assert len(list(stream.tail(config.local(), limit=3, seconds=None))) == 3
    assert events["closed"] is True


def test_the_clock_stops_the_tail_on_a_quiet_topic(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [])  # poll always returns None
    assert list(stream.tail(config.local(), limit=None, seconds=0.15)) == []
    assert events["closed"] is True


def test_kafka_metadata_is_attached_to_each_record(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage(partition=3, offset=42)])
    (decoded,) = list(stream.tail(config.local(), limit=1, seconds=None))
    assert decoded["kafka_partition"] == 3
    assert decoded["kafka_offset"] == 42
    assert decoded["kafka_timestamp_ms"] == 1_700_000_000_123
    assert decoded["kafka_key"] == "binance|BTCUSDT"
    # and the Avro payload is still intact alongside it
    assert decoded["venue"] == "binance"


def test_metadata_keys_cannot_collide_with_the_schema(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage()])
    (decoded,) = list(stream.tail(config.local(), limit=1, seconds=None))
    avro_fields = set(record()) - {k for k in decoded if k.startswith("kafka_")}
    assert not any(field.startswith("kafka_") for field in avro_fields)


def test_a_null_key_does_not_blow_up(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage(key=None)])
    (decoded,) = list(stream.tail(config.local(), limit=1, seconds=None))
    assert decoded["kafka_key"] is None


def test_an_undecodable_record_names_its_offset(monkeypatch):
    events: dict[str, Any] = {}

    class Exploding:
        def decode(self, data: bytes) -> dict[str, Any]:
            raise ValueError("not avro")

    monkeypatch.setattr(stream, "consumer", lambda *a, **k: _fake_consumer(events, [FakeMessage()]))
    monkeypatch.setattr(stream, "trade_codec", Exploding)
    with pytest.raises(ValueError, match=r"md\.trades\.v1\[3\]@42"):
        list(stream.tail(config.local(), limit=1, seconds=None))
    assert events["closed"] is True


def test_the_consumer_is_closed_when_the_caller_abandons_the_tail(monkeypatch):
    # Notebook users interrupt cells constantly; a leaked consumer holds its
    # group membership and partition assignment until the broker times out.
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage() for _ in range(10)])
    generator = stream.tail(config.local(), limit=10, seconds=None)
    next(generator)  # start it, so the context manager is actually entered
    assert "closed" not in events
    generator.close()
    assert events["closed"] is True


def test_collect_drains_the_tail(monkeypatch):
    events: dict[str, Any] = {}
    patch_stream(monkeypatch, events, [FakeMessage() for _ in range(5)])
    assert len(stream.collect(config.local(), limit=5, seconds=None)) == 5
