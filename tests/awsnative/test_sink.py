from __future__ import annotations

import json
from typing import Any

import pytest

from awsnative.sink import KinesisSink
from ingest.core.models import Side, Source, Trade


def make_trade(trade_id: str = "1", symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        venue="binance",
        venue_symbol=symbol,
        instrument_id="BTC-USD",
        trade_id=trade_id,
        event_ts_us=1_754_000_000_000_000,
        ingest_ts_us=1_754_000_000_100_000,
        price="61234.56",
        size="0.0123",
        side=Side.BUY,
        sequence=int(trade_id),
        is_backfill=False,
        source=Source.STREAM,
    )


class FakeKinesis:
    """Records every put_records call and replays a scripted set of outcomes."""

    def __init__(self, failures: list[set[int]] | None = None) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        # failures[n] = indices within call n that should fail
        self._failures = failures or []

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> dict[str, Any]:
        self.stream_name = StreamName
        self.calls.append(Records)
        call_index = len(self.calls) - 1
        failing = self._failures[call_index] if call_index < len(self._failures) else set()
        return {
            "FailedRecordCount": len(failing),
            "Records": [
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "slow down"}
                if i in failing
                else {"ShardId": "shardId-000000000000", "SequenceNumber": str(i)}
                for i in range(len(Records))
            ],
        }


def make_sink(fake: FakeKinesis, **kwargs: Any) -> tuple[KinesisSink, list[float]]:
    slept: list[float] = []
    sink = KinesisSink(
        "fdai-native-md-trades-v1",
        client=fake,
        sleep=slept.append,
        **kwargs,
    )
    return sink, slept


def test_produce_buffers_and_does_not_call_aws() -> None:
    """produce must never block on the network -- IngestRunner calls it from the
    frame-parsing path."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake)

    sink.produce("md.trades.v1", make_trade())

    assert fake.calls == []


def test_flush_sends_the_buffer_and_empties_it() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade("1"))
    sink.produce("md.trades.v1", make_trade("2"))

    pending = sink.flush()

    assert pending == 0
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 2
    assert fake.stream_name == "fdai-native-md-trades-v1"


def test_partition_key_is_venue_pipe_symbol() -> None:
    """Same key as the Kafka path, so one instrument on one venue stays ordered."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade(symbol="ETHUSDT"))
    sink.flush()

    assert fake.calls[0][0]["PartitionKey"] == "binance|ETHUSDT"


def test_record_data_is_the_json_encoding() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake)
    sink.produce("md.trades.v1", make_trade("42"))
    sink.flush()

    payload = json.loads(fake.calls[0][0]["Data"].decode())
    assert payload["trade_id"] == "42"
    assert payload["schema_version"] == 1


def test_poll_flushes_once_the_batch_is_full() -> None:
    """poll is called after every produce in IngestRunner.drain, which makes it
    the natural place to decide whether to send."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=3)

    for i in range(3):
        sink.produce("md.trades.v1", make_trade(str(i)))
        sink.poll(0)

    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 3


def test_poll_does_not_flush_a_partial_batch() -> None:
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=10)

    sink.produce("md.trades.v1", make_trade())
    sink.poll(0)

    assert fake.calls == []


def test_only_failed_records_are_retried() -> None:
    """The Kinesis gotcha: put_records returns 200 with a partial failure, and
    the failed records are lost unless resent. Resending the whole batch would
    duplicate the successes."""
    fake = FakeKinesis(failures=[{1}])  # second record of the first call fails
    sink, slept = make_sink(fake)
    for i in range(3):
        sink.produce("md.trades.v1", make_trade(str(i)))

    pending = sink.flush()

    assert pending == 0
    assert len(fake.calls) == 2
    assert len(fake.calls[1]) == 1
    retried = json.loads(fake.calls[1][0]["Data"].decode())
    assert retried["trade_id"] == "1"
    assert slept, "a throttled retry must back off before resending"


def test_backoff_grows_between_attempts() -> None:
    fake = FakeKinesis(failures=[{0}, {0}, {0}])
    sink, slept = make_sink(fake)
    sink.produce("md.trades.v1", make_trade())

    sink.flush()

    assert len(slept) >= 3
    assert slept[1] > slept[0]


def test_exhausting_retries_raises_rather_than_dropping() -> None:
    """Silent trade loss is the one failure this system must not have. Raising
    surfaces as queue backpressure, which BoundedTopicQueue's BLOCK policy turns
    into a detectable, REST-repairable gap."""
    fake = FakeKinesis(failures=[{0}] * 10)
    sink, _ = make_sink(fake, max_attempts=3)
    sink.produce("md.trades.v1", make_trade())

    with pytest.raises(RuntimeError, match="1 record"):
        sink.flush()


def test_oversized_buffer_flushes_on_bytes_not_just_count() -> None:
    """put_records caps at 5MB per request; exceeding it fails the whole call."""
    fake = FakeKinesis()
    sink, _ = make_sink(fake, max_batch=500, max_bytes=700)

    for i in range(4):
        sink.produce("md.trades.v1", make_trade(str(i)))
        sink.poll(0)

    assert fake.calls, "should have flushed on the byte threshold"
    assert all(sum(len(r["Data"]) for r in call) <= 700 + 400 for call in fake.calls), (
        "each request must respect the byte cap"
    )
