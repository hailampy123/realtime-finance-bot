"""The transport seam.

IngestRunner has always depended on a structural protocol rather than on Kafka,
so a second transport needs a second implementation and no change to the runner.
The protocol's shape is Kafka-derived and that turns out to fit: `poll` is called
after every `produce` in IngestRunner.drain, which is exactly the hook a batching
sink needs to decide whether to flush.

Implementations:
  ingest.core.producer.TradeProducer  -- Kafka/MSK, bare Avro datums
  awsnative.sink.KinesisSink          -- Kinesis Data Streams, JSON
"""

from __future__ import annotations

from typing import Protocol

from ingest.core.models import Trade


class Sink(Protocol):
    def produce(self, topic: str, trade: Trade) -> None:
        """Hand a trade to the transport. May buffer; must not block on the network."""
        ...

    def poll(self, timeout: float = 0.0) -> int:
        """Service the transport. Called after every produce and on drain idle.

        Returns the number of events serviced.
        """
        ...

    def flush(self, timeout: float = 10.0) -> int:
        """Block until buffered records are delivered. Returns the number still pending."""
        ...
