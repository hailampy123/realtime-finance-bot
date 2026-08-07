from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Callable

import structlog

from ingest.connectors.base import Connector
from ingest.connectors.binance import BinanceConnector
from ingest.connectors.coinbase import CoinbaseConnector
from ingest.core.gaps import SequenceTracker
from ingest.core.instruments import InstrumentMap
from ingest.core.producer import TradeProducer
from ingest.core.queue import TOPIC_POLICIES, BoundedTopicQueue
from ingest.runner import IngestRunner
from ingest.settings import Settings

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
    settings = Settings()
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
            TradeProducer(
                settings.bootstrap_servers,
                sasl_username=settings.sasl_username,
                sasl_password=settings.sasl_password,
            ),
            SequenceTracker(),
            BoundedTopicQueue(settings.queue_maxsize, TOPIC_POLICIES[settings.trades_topic]),
            settings.trades_topic,
        )
        log.info("starting_venue", venue=venue, symbols=len(symbols))
        tasks.append(asyncio.create_task(runner.run(stop, symbols)))

    if not tasks:
        raise SystemExit("no venues to run")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
