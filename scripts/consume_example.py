"""Consume live trades and compute a rolling per-instrument VWAP.

A minimal example of reading the bare-Avro trade stream and doing something
with it locally — no Databricks/Spark required. Decodes with the same
trade_codec() the producer and smoke test use, so there is exactly one
schema in play.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict, deque
from decimal import Decimal

from confluent_kafka import Consumer

from ingest.core.codec import trade_codec


def vwap(window: deque[tuple[Decimal, Decimal]]) -> Decimal:
    notional = sum((price * size for price, size in window), Decimal(0))
    volume = sum((size for _, size in window), Decimal(0))
    return notional / volume if volume else Decimal(0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--topic", default="md.trades.v1")
    parser.add_argument("--group", default="consume-example")
    parser.add_argument("--offset-reset", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--window", type=int, default=50, help="trades per rolling VWAP window")
    parser.add_argument(
        "--print-every", type=int, default=20, help="print a snapshot every N trades"
    )
    args = parser.parse_args()

    config: dict[str, object] = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": args.offset_reset,
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

    windows: dict[str, deque[tuple[Decimal, Decimal]]] = defaultdict(
        lambda: deque(maxlen=args.window)
    )
    seen = 0

    print(f"consuming {args.topic} from {args.bootstrap} (Ctrl-C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", file=sys.stderr)
                continue

            record = codec.decode(msg.value())
            instrument_id = record["instrument_id"]
            windows[instrument_id].append((Decimal(record["price"]), Decimal(record["size"])))
            seen += 1

            if seen % args.print_every == 0:
                print(f"--- {seen} trades seen, {time.strftime('%H:%M:%S')} ---")
                for instrument_id, window in sorted(windows.items()):
                    print(f"  {instrument_id:10s} vwap={vwap(window):.2f} (n={len(window)})")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
