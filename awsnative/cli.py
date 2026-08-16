"""AWS-native producer entrypoint.

Deliberately a separate module from ingest.cli rather than a transport flag on
it: the Kafka path must keep starting with no AWS configuration present, and
the container image selects a transport by choosing a command.

Everything below the sink is shared -- same connectors, same gap detection and
REST repair, same backpressure policy -- which is what makes the two
workstreams comparable rather than merely similar.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Callable

import structlog

from awsnative.settings import NativeSettings
from awsnative.sink import KinesisSink
from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue
from ingest.runner import IngestRunner

log = structlog.get_logger(__name__)

CONNECTORS: dict[str, Callable[[InstrumentMap], Connector]] = {
    "binance": BinanceConnector,
    "coinbase": CoinbaseConnector,
}


def build_connector(venue: str, instruments: InstrumentMap) -> Connector:
    try:
        return CONNECTORS[venue](instruments)
    except KeyError:
        raise SystemExit(f"unknown venue {venue!r}; known: {sorted(CONNECTORS)}") from None


async def main() -> None:
    structlog.configure(processors=[structlog.processors.JSONRenderer()])
    settings = NativeSettings()
    instruments = InstrumentMap.from_yaml(settings.universe_path)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    tasks = []
    for venue in settings.venues:
        connector = build_connector(venue, instruments)
        symbols = instruments.symbols_for(venue)
        if not symbols:
            log.warning("no_symbols_configured", venue=venue)
            continue
        runner = IngestRunner(
            connector,
            # One sink per venue: each has its own buffer, so a throttled retry
            # on one venue's batch never stalls the other's drain task.
            KinesisSink(settings.stream_name),
            SequenceTracker(),
            BoundedTopicQueue(settings.queue_maxsize, TOPIC_POLICIES[settings.trades_topic]),
            settings.trades_topic,
        )
        log.info(
            "starting_venue",
            venue=venue,
            symbols=len(symbols),
            stream=settings.stream_name,
        )
        tasks.append(asyncio.create_task(runner.run(stop, symbols)))

    if not tasks:
        raise SystemExit("no venues to run")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
