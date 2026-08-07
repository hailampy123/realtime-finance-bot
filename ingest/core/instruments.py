from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class InstrumentMap:
    """Maps venue-specific tickers onto one canonical instrument id.

    BTCUSDT (binance) and BTC-USD (coinbase) are the same instrument; Silver
    joins on the canonical id, so the mapping has to live upstream of Kafka.
    """

    _to_canonical: dict[tuple[str, str], str]
    _by_venue: dict[str, list[str]]

    @classmethod
    def from_yaml(cls, path: Path) -> InstrumentMap:
        doc = yaml.safe_load(path.read_text())
        to_canonical: dict[tuple[str, str], str] = {}
        by_venue: dict[str, list[str]] = {}
        for entry in doc["instruments"]:
            for venue, venue_symbol in entry["venues"].items():
                to_canonical[(venue, venue_symbol)] = entry["id"]
                by_venue.setdefault(venue, []).append(venue_symbol)
        return cls(to_canonical, by_venue)

    def canonical(self, venue: str, venue_symbol: str) -> str:
        try:
            return self._to_canonical[(venue, venue_symbol)]
        except KeyError:
            raise KeyError(f"unmapped instrument {venue}:{venue_symbol}") from None

    def symbols_for(self, venue: str) -> list[str]:
        return list(self._by_venue.get(venue, []))
