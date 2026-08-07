from __future__ import annotations

from collections.abc import Callable
from typing import Any

import structlog

from ingest.core.codec import TRADE_SCHEMA_VERSION, trade_codec
from ingest.core.models import Trade

log = structlog.get_logger(__name__)


def build_config(
    bootstrap_servers: str,
    sasl_username: str | None,
    sasl_password: str | None,
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 20,
        "batch.size": 262_144,
        "max.in.flight.requests.per.connection": 5,
        "retries": 10_000_000,
        "delivery.timeout.ms": 300_000,
    }
    if sasl_username and sasl_password:
        config |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
        }
    return config


def _default_factory(config: dict[str, Any]) -> Any:
    from confluent_kafka import Producer

    return Producer(config)


class TradeProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        *,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        factory = producer_factory or _default_factory
        self._producer = factory(build_config(bootstrap_servers, sasl_username, sasl_password))
        self._codec = trade_codec()

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            log.error("kafka_delivery_failed", error=str(err))

    def produce(self, topic: str, trade: Trade) -> None:
        self._producer.produce(
            topic,
            key=trade.kafka_key(),
            value=self._codec.encode(trade.to_avro()),
            headers=[
                ("schema_version", str(TRADE_SCHEMA_VERSION).encode()),
                ("venue", trade.venue.encode()),
                ("is_backfill", b"true" if trade.is_backfill else b"false"),
                ("source", str(trade.source).encode()),
            ],
            on_delivery=self._on_delivery,
        )

    def poll(self, timeout: float = 0.0) -> int:
        return int(self._producer.poll(timeout))

    def flush(self, timeout: float = 10.0) -> int:
        return int(self._producer.flush(timeout))
