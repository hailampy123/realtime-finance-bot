"""Bounded reads off a trade topic.

Every read here is bounded by both a message count and a wall clock, and at
least one of the two must be set. That is not defensiveness for its own sake:
the characteristic failure of Kafka-in-a-notebook is a cell that polls forever
against a quiet topic with no output and no way to tell whether it is waiting
on the broker, the network, or an empty partition.

Decoding goes through `ingest.core.codec.trade_codec()` — the same codec the
producer encodes with — so there is exactly one schema in play here too.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from confluent_kafka import Consumer, KafkaException

from devlab.config import Target
from ingest.core.codec import trade_codec

DEFAULT_TOPIC = "md.trades.v1"
METADATA_TIMEOUT_S = 15.0


class TopicMissing(RuntimeError):
    """The topic does not exist on the broker."""


def _check_topic(client: Any, topic: str, target: Target) -> None:
    """Fail fast and specifically instead of polling an absent topic forever.

    The local broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, so a
    fresh `make compose-up` has *no* topics at all — the single most likely
    reason a notebook sees nothing.
    """
    try:
        metadata = client.list_topics(timeout=METADATA_TIMEOUT_S)
    except KafkaException as exc:
        raise TopicMissing(
            f"could not reach the broker at {target.bootstrap!r} (target {target.name!r}): {exc}"
        ) from exc
    if topic in metadata.topics:
        return
    known = sorted(t for t in metadata.topics if not t.startswith("__"))
    hint = (
        "run `make stream-local` to create the topics and start the producers"
        if target.name == "local"
        else "run `make up` to provision the cluster and its topics"
    )
    raise TopicMissing(
        f"topic {topic!r} does not exist on {target.bootstrap!r}; {hint}. "
        f"Topics present: {known or '(none)'}"
    )


@contextmanager
def consumer(
    target: Target,
    topic: str = DEFAULT_TOPIC,
    *,
    group: str | None = None,
    offset_reset: str = "latest",
) -> Iterator[Any]:
    """A subscribed consumer that always gets closed.

    The group id is random per call by default. A stable group would resume
    from a committed offset, so re-running a cell would show nothing and look
    like a dead stream.
    """
    group = group or f"devlab-{uuid.uuid4().hex[:8]}"
    client = Consumer(target.consumer_config(group=group, offset_reset=offset_reset))
    try:
        _check_topic(client, topic, target)
        client.subscribe([topic])
        yield client
    finally:
        client.close()


def tail(
    target: Target,
    topic: str = DEFAULT_TOPIC,
    *,
    limit: int | None = 100,
    seconds: float | None = 30.0,
    offset_reset: str = "latest",
    group: str | None = None,
    poll_timeout: float = 1.0,
) -> Iterator[dict[str, Any]]:
    """Yield decoded trades until `limit` messages or `seconds` elapse.

    Each record is the Avro payload plus `kafka_*` keys for partition, offset,
    and broker timestamp — the schema has no field starting with `kafka_`, so
    they cannot collide.

    Set `offset_reset="earliest"` to read what is already retained rather than
    only what arrives from now on.
    """
    if limit is None and seconds is None:
        raise ValueError("set at least one of limit= or seconds=; an unbounded tail cannot stop")
    if limit is not None and limit <= 0:
        raise ValueError(f"limit must be positive, got {limit}")

    codec = trade_codec()
    deadline = None if seconds is None else time.monotonic() + seconds
    seen = 0

    with consumer(target, topic, group=group, offset_reset=offset_reset) as client:
        while True:
            if limit is not None and seen >= limit:
                return
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                wait = min(poll_timeout, remaining)
            else:
                wait = poll_timeout

            message = client.poll(wait)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            try:
                record: dict[str, Any] = codec.decode(message.value())
            except Exception as exc:
                raise ValueError(
                    f"could not decode {topic}[{message.partition()}]@{message.offset()} "
                    f"as a Trade record — is this topic carrying trade data?"
                ) from exc

            _, timestamp_ms = message.timestamp()
            key = message.key()
            record |= {
                "kafka_partition": message.partition(),
                "kafka_offset": message.offset(),
                "kafka_timestamp_ms": timestamp_ms,
                "kafka_key": key.decode() if key is not None else None,
            }
            seen += 1
            yield record


def collect(
    target: Target,
    topic: str = DEFAULT_TOPIC,
    *,
    limit: int | None = 500,
    seconds: float | None = 30.0,
    offset_reset: str = "latest",
    group: str | None = None,
) -> list[dict[str, Any]]:
    """`tail()` drained into a list. The usual entry point for analysis cells."""
    return list(
        tail(
            target,
            topic,
            limit=limit,
            seconds=seconds,
            offset_reset=offset_reset,
            group=group,
        )
    )
