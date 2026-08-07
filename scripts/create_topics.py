"""Create the topic set. Idempotent: existing topics are left alone."""

from __future__ import annotations

import argparse
import sys
from typing import NamedTuple

from confluent_kafka.admin import AdminClient, NewTopic

HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS


class TopicSpec(NamedTuple):
    name: str
    partitions: int
    retention_ms: int


TOPIC_SPECS: list[TopicSpec] = [
    TopicSpec("md.trades.v1", 6, 24 * HOUR_MS),
    TopicSpec("md.book.top.v1", 6, 6 * HOUR_MS),
    TopicSpec("md.book.depth.v1", 6, 2 * HOUR_MS),
    TopicSpec("md.bars.v1", 3, 48 * HOUR_MS),
    TopicSpec("news.articles.v1", 3, 7 * DAY_MS),
    TopicSpec("ops.metrics.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.trades.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.book.top.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.book.depth.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.md.bars.v1", 1, 7 * DAY_MS),
    TopicSpec("_dlq.news.articles.v1", 1, 7 * DAY_MS),
]


def to_new_topic(spec: TopicSpec, replication_factor: int) -> NewTopic:
    return NewTopic(
        spec.name,
        num_partitions=spec.partitions,
        replication_factor=replication_factor,
        config={
            "retention.ms": str(spec.retention_ms),
            "compression.type": "zstd",
            "min.insync.replicas": str(max(1, replication_factor - 1)),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--replication-factor", type=int, default=2)
    args = parser.parse_args()

    config: dict[str, object] = {"bootstrap.servers": args.bootstrap}
    if args.username and args.password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": args.username,
            "sasl.password": args.password,
        }

    admin = AdminClient(config)
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [s for s in TOPIC_SPECS if s.name not in existing]
    if not wanted:
        print("all topics already exist")
        return 0

    futures = admin.create_topics([to_new_topic(s, args.replication_factor) for s in wanted])
    failed = 0
    for name, future in futures.items():
        try:
            future.result(timeout=60)
            print(f"created {name}")
        except Exception as exc:
            print(f"FAILED {name}: {exc}", file=sys.stderr)
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
