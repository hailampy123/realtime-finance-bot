"""Is the stream alive, and is it complete?

Three separate questions, answered separately:

* `topics()` / `partitions()` — is there anything in the log at all?
* `rate()` — is anything arriving *now*, and from which venue?
* `sequence_gaps()` — did we miss anything in between?

Returns plain dataclasses rather than DataFrames so this module stays usable
without pandas. Pass the results to `devlab.frames.frame()` for a table.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from devlab.config import Target
from devlab.stream import DEFAULT_TOPIC, tail
from ingest.core.gaps import Gap, SequenceTracker

METADATA_TIMEOUT_S = 15.0
WATERMARK_TIMEOUT_S = 10.0

# Coinbase's sequence_num is connection-wide rather than per product
# (see CoinbaseConnector.sequence_symbol), and trades are partitioned by
# venue|symbol. Kafka only orders within a partition, so a connection-wide
# counter read back from a multi-partition topic is interleaved by
# construction, and every "gap" it reports would be an artefact of the read
# rather than a real loss. Checking it here would produce confident nonsense.
CONNECTION_SCOPED_SEQUENCE_VENUES = frozenset({"coinbase"})


@dataclass(frozen=True, slots=True)
class PartitionInfo:
    topic: str
    partition: int
    low: int
    high: int

    @property
    def messages(self) -> int:
        return self.high - self.low


@dataclass(frozen=True, slots=True)
class TopicInfo:
    name: str
    partitions: int
    messages: int


@dataclass(frozen=True, slots=True)
class LagInfo:
    topic: str
    partition: int
    committed: int | None
    high: int

    @property
    def lag(self) -> int | None:
        return None if self.committed is None else self.high - self.committed


@dataclass(frozen=True, slots=True)
class RateReport:
    topic: str
    messages: int
    seconds: float
    by_venue: dict[str, int]
    by_instrument: dict[str, int]

    @property
    def per_second(self) -> float:
        return self.messages / self.seconds if self.seconds > 0 else 0.0


@dataclass(frozen=True, slots=True)
class GapReport:
    checked: int
    gaps: list[Gap]
    skipped_venues: list[str]

    @property
    def missing(self) -> int:
        return sum(gap.missing_count for gap in self.gaps)


def _watermarks(client: Any, topic: str, partition_ids: list[int]) -> list[PartitionInfo]:
    infos = []
    for partition_id in sorted(partition_ids):
        low, high = client.get_watermark_offsets(
            TopicPartition(topic, partition_id), timeout=WATERMARK_TIMEOUT_S
        )
        infos.append(PartitionInfo(topic=topic, partition=partition_id, low=low, high=high))
    return infos


def topics(target: Target, *, prefix: str = "", include_internal: bool = False) -> list[TopicInfo]:
    """Every topic on the broker with its partition count and retained messages.

    `messages` is `high - low` summed across partitions, so it counts what is
    currently retained, not what was ever written — `md.trades.v1` ages out at
    24h by design.
    """
    admin = AdminClient(target.admin_config())
    metadata = admin.list_topics(timeout=METADATA_TIMEOUT_S)
    client = Consumer(target.consumer_config(group="devlab-health"))
    try:
        result = []
        for name, topic_metadata in sorted(metadata.topics.items()):
            if not include_internal and name.startswith("__"):
                continue
            if not name.startswith(prefix):
                continue
            infos = _watermarks(client, name, list(topic_metadata.partitions))
            result.append(
                TopicInfo(
                    name=name,
                    partitions=len(infos),
                    messages=sum(info.messages for info in infos),
                )
            )
        return result
    finally:
        client.close()


def partitions(target: Target, topic: str = DEFAULT_TOPIC) -> list[PartitionInfo]:
    """Per-partition low/high watermarks. Uneven `messages` means uneven keying."""
    client = Consumer(target.consumer_config(group="devlab-health"))
    try:
        metadata = client.list_topics(topic=topic, timeout=METADATA_TIMEOUT_S)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or not topic_metadata.partitions:
            raise ValueError(f"topic {topic!r} not found on {target.bootstrap!r}")
        return _watermarks(client, topic, list(topic_metadata.partitions))
    finally:
        client.close()


def lag(target: Target, group: str, topic: str = DEFAULT_TOPIC) -> list[LagInfo]:
    """How far behind a named consumer group is.

    `committed` is None for a partition the group has never committed — which
    is the normal state for `devlab`'s own reads, since they run with
    auto-commit off.
    """
    client = Consumer(target.consumer_config(group=group))
    try:
        metadata = client.list_topics(topic=topic, timeout=METADATA_TIMEOUT_S)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or not topic_metadata.partitions:
            raise ValueError(f"topic {topic!r} not found on {target.bootstrap!r}")
        partition_ids = sorted(topic_metadata.partitions)
        committed = client.committed(
            [TopicPartition(topic, pid) for pid in partition_ids],
            timeout=METADATA_TIMEOUT_S,
        )
        by_partition = {tp.partition: tp.offset for tp in committed}
        result = []
        for info in _watermarks(client, topic, partition_ids):
            offset = by_partition.get(info.partition)
            # confluent_kafka reports "never committed" as OFFSET_INVALID (-1001).
            result.append(
                LagInfo(
                    topic=topic,
                    partition=info.partition,
                    committed=None if offset is None or offset < 0 else offset,
                    high=info.high,
                )
            )
        return result
    finally:
        client.close()


def rate(target: Target, topic: str = DEFAULT_TOPIC, *, seconds: float = 10.0) -> RateReport:
    """Sample the live arrival rate for `seconds`, broken down by venue.

    Reads from `latest`, so this measures what is arriving now — a zero here
    with a non-zero `topics()` count means the producers stopped, not that the
    topic is empty.
    """
    started = time.monotonic()
    venues: Counter[str] = Counter()
    instruments: Counter[str] = Counter()
    total = 0
    for record in tail(target, topic, limit=None, seconds=seconds, offset_reset="latest"):
        venues[record["venue"]] += 1
        instruments[record["instrument_id"]] += 1
        total += 1
    return RateReport(
        topic=topic,
        messages=total,
        seconds=time.monotonic() - started,
        by_venue=dict(venues.most_common()),
        by_instrument=dict(instruments.most_common()),
    )


def sequence_gaps(records: list[dict[str, Any]]) -> GapReport:
    """Replay sequence numbers through the producer's own gap detector.

    Only venues whose sequence is scoped per symbol can be checked this way;
    see CONNECTION_SCOPED_SEQUENCE_VENUES for why Coinbase is excluded rather
    than reported as clean.

    A gap here is weaker evidence than one from `IngestRunner`: it means the
    record never reached Kafka *or* was not retained, and the runner may
    already have repaired it via REST under a later offset.
    """
    tracker = SequenceTracker()
    found: list[Gap] = []
    checked = 0
    skipped: set[str] = set()
    for record in records:
        venue = record["venue"]
        if venue in CONNECTION_SCOPED_SEQUENCE_VENUES:
            skipped.add(venue)
            continue
        if record.get("sequence") is None:
            continue
        gap = tracker.observe(venue, record["venue_symbol"], record["sequence"])
        checked += 1
        if gap is not None:
            found.append(gap)
    return GapReport(checked=checked, gaps=found, skipped_venues=sorted(skipped))
