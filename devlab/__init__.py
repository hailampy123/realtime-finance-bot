"""Interactive helpers for poking at the live trade streams from a notebook.

`devlab` is the dev-loop counterpart to `ingest`: `ingest` writes to Kafka,
`devlab` reads back out of it. It exists so the three notebooks under
`notebooks/` stay thin — one shared consumer, one shared target resolver, one
shared decoder — rather than each carrying its own copy of the SASL block that
drifts the moment the cluster is rebuilt.

Nothing here is on the production path. It is not copied into the producer
image (see docker/Dockerfile), and its dependencies live in the opt-in
`notebook` group, so `uv sync` and `make check` never install them.

`devlab.frames` is deliberately *not* imported here: it needs pandas, which
only the `notebook` group provides. Import it explicitly when you want it.
"""

from __future__ import annotations

from devlab.config import Target, from_terraform, local, msk, resolve
from devlab.health import PartitionInfo, RateReport, TopicInfo, partitions, rate, topics
from devlab.stream import DEFAULT_TOPIC, collect, consumer, tail

__all__ = [
    "DEFAULT_TOPIC",
    "PartitionInfo",
    "RateReport",
    "Target",
    "TopicInfo",
    "collect",
    "consumer",
    "from_terraform",
    "local",
    "msk",
    "partitions",
    "rate",
    "resolve",
    "tail",
    "topics",
]
