"""KinesisSink -- the AWS-native implementation of ingest.core.sinks.Sink.

Two behaviours here are load-bearing rather than incidental.

Partial failures. put_records returns HTTP 200 with a FailedRecordCount and
per-record error codes; the failed records are gone unless resent. Resending
the whole batch would duplicate the successes, so only the failed subset is
retried, matched positionally against the request.

Never dropping a trade. When retries are exhausted this raises instead of
discarding. The exception propagates to IngestRunner.drain and stops the drain,
which fills the BoundedTopicQueue, whose BLOCK policy for trades makes the
producer block and take a gap -- and a gap is detectable and REST-repairable,
where a silent drop is not. That chain is why the parent spec can claim trades
are never silently lost, and it is inherited unchanged from the Kafka path.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

import structlog

from awsnative.encode import encode_trade
from ingest.core.models import Trade

log = structlog.get_logger(__name__)

# put_records hard limits: 500 records and 5 MiB per request. The byte cap is
# set below 5 MiB so a batch assembled right up to the threshold plus one more
# record still fits.
_MAX_RECORDS_PER_REQUEST = 500
_MAX_BYTES_PER_REQUEST = 4_500_000

# Throttling is expected on an on-demand stream: it doubles capacity within
# ~15 minutes of a sustained increase, so an instantaneous spike above current
# capacity is throttled regardless of stream mode.
_BASE_BACKOFF_S = 0.05
_MAX_BACKOFF_S = 5.0


def _default_client() -> Any:
    import boto3

    return boto3.client("kinesis")


class KinesisSink:
    def __init__(
        self,
        stream_name: str,
        *,
        client: Any | None = None,
        max_batch: int = _MAX_RECORDS_PER_REQUEST,
        max_bytes: int = _MAX_BYTES_PER_REQUEST,
        max_attempts: int = 8,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._stream_name = stream_name
        self._client = client if client is not None else _default_client()
        self._max_batch = max_batch
        self._max_bytes = max_bytes
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._buffer: list[dict[str, Any]] = []
        self._buffered_bytes = 0

    # -- Sink protocol -------------------------------------------------------

    def produce(self, topic: str, trade: Trade) -> None:
        """Buffer only. IngestRunner calls this from the frame-parsing path, so
        it must not touch the network. `topic` is accepted for protocol
        compatibility and ignored: a Kinesis stream is the topic."""
        data = encode_trade(trade)
        self._buffer.append({"Data": data, "PartitionKey": trade.kafka_key().decode()})
        self._buffered_bytes += len(data)

    def poll(self, timeout: float = 0.0) -> int:
        """Send if a threshold is reached. IngestRunner.drain calls this after
        every produce, which makes it the batching trigger."""
        if len(self._buffer) >= self._max_batch or self._buffered_bytes >= self._max_bytes:
            return self._send_all()
        return 0

    def flush(self, timeout: float = 10.0) -> int:
        """Send everything buffered. Returns records still pending (always 0 --
        anything undeliverable raises)."""
        self._send_all()
        return len(self._buffer)

    # -- internals -----------------------------------------------------------

    def _send_all(self) -> int:
        sent = 0
        while self._buffer:
            batch, self._buffer = self._take_batch()
            self._buffered_bytes = sum(len(r["Data"]) for r in self._buffer)
            sent += self._send_with_retry(batch)
        return sent

    def _take_batch(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Slice off one request-sized batch, respecting both caps."""
        batch: list[dict[str, Any]] = []
        size = 0
        for index, record in enumerate(self._buffer):
            record_size = len(record["Data"])
            if batch and (len(batch) >= self._max_batch or size + record_size > self._max_bytes):
                return batch, self._buffer[index:]
            batch.append(record)
            size += record_size
        return batch, []

    def _send_with_retry(self, records: list[dict[str, Any]]) -> int:
        pending = records
        for attempt in range(self._max_attempts):
            if attempt:
                # Full jitter: a fixed backoff makes every retrying producer
                # collide on the same instant.
                delay = min(_MAX_BACKOFF_S, _BASE_BACKOFF_S * (2**attempt))
                self._sleep(random.uniform(0, delay))

            response = self._client.put_records(StreamName=self._stream_name, Records=pending)
            failed_count = int(response.get("FailedRecordCount", 0) or 0)
            if not failed_count:
                return len(pending)

            results = response.get("Records", [])
            pending = [
                record
                for record, result in zip(pending, results, strict=False)
                if result.get("ErrorCode")
            ]
            log.warning(
                "kinesis_partial_failure",
                failed=len(pending),
                attempt=attempt + 1,
                error_code=next((r.get("ErrorCode") for r in results if r.get("ErrorCode")), None),
            )
            if not pending:
                return len(records)

        raise RuntimeError(
            f"kinesis put_records failed for {len(pending)} record(s) after "
            f"{self._max_attempts} attempts; refusing to drop trades"
        )


def _assert_protocol() -> None:
    """mypy fails here if KinesisSink drifts from the Sink protocol."""
    from ingest.core.sinks import Sink

    sink: Sink = KinesisSink("x", client=object())
    _ = sink
