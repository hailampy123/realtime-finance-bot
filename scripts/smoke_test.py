"""Assert that live trades are actually flowing. Exit non-zero if not."""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka import Consumer

from ingest.core.codec import trade_codec


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--topic", default="md.trades.v1")
    parser.add_argument("--min-messages", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    config: dict[str, object] = {
        "bootstrap.servers": args.bootstrap,
        "group.id": f"smoke-{int(time.time())}",
        "auto.offset.reset": "latest",
    }
    if args.username and args.password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": args.username,
            "sasl.password": args.password,
        }

    consumer = Consumer(config)
    consumer.subscribe([args.topic])
    codec = trade_codec()

    seen = 0
    deadline = time.monotonic() + args.timeout
    while seen < args.min_messages and time.monotonic() < deadline:
        msg = consumer.poll(2.0)
        if msg is None or msg.error():
            continue
        record = codec.decode(msg.value())
        print(f"{record['venue']} {record['venue_symbol']} {record['price']} {record['side']}")
        seen += 1
    consumer.close()

    if seen < args.min_messages:
        print(f"SMOKE FAIL: saw {seen}/{args.min_messages} messages", file=sys.stderr)
        return 1
    print(f"SMOKE OK: {seen} live trades decoded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
