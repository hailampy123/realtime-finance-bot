"""Decoded records to pandas, plus the two transforms Silver will need.

`dedupe()` and `bars()` are deliberately written as the pandas equivalents of
the lakehouse contracts in the data-layer spec, so this module doubles as a
place to get the semantics right before they are rewritten as Structured
Streaming. Each carries a note on how it maps to PySpark.

Requires pandas, which lives in the opt-in `notebook` dependency group:

    uv sync --group notebook
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any

import pandas as pd

# The natural key the Silver layer dedupes on. Repair and archive backfill both
# re-emit trades the stream may already have delivered, so the same trade can
# legitimately appear more than once in the log.
NATURAL_KEY = ["venue", "venue_symbol", "trade_id"]

TRADE_COLUMNS = [
    "event_ts",
    "ingest_ts",
    "venue",
    "venue_symbol",
    "instrument_id",
    "trade_id",
    "price",
    "size",
    "notional",
    "side",
    "sequence",
    "source",
    "is_backfill",
    "latency_ms",
    "price_str",
    "size_str",
    "kafka_partition",
    "kafka_offset",
]


def _row(obj: Any) -> dict[str, Any]:
    if is_dataclass(obj) and not isinstance(obj, type):
        row = asdict(obj)
        # asdict() drops properties, but on these dataclasses the properties are
        # the interesting columns (messages, lag, per_second).
        for name, attribute in vars(type(obj)).items():
            if isinstance(attribute, property) and not name.startswith("_"):
                row[name] = getattr(obj, name)
        return row
    if isinstance(obj, Mapping):
        return dict(obj)
    raise TypeError(f"expected a dataclass or mapping, got {type(obj)!r}")


def frame(objs: Iterable[Any]) -> pd.DataFrame:
    """Dataclasses or dicts to a DataFrame, keeping dataclass properties."""
    return pd.DataFrame([_row(obj) for obj in objs])


def trades_frame(records: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Decoded trade records to a typed, time-sorted DataFrame.

    Prices and sizes cross the wire as strings so no precision is lost in
    transit. They are converted to float64 here because that is what pandas
    arithmetic and plotting need — the exact originals are kept alongside as
    `price_str` / `size_str`, which is what to check when a number looks wrong.
    """
    if not records:
        return pd.DataFrame(columns=TRADE_COLUMNS)

    df = pd.DataFrame(list(records))
    df["price_str"] = df["price"]
    df["size_str"] = df["size"]
    df["price"] = df["price"].astype("float64")
    df["size"] = df["size"].astype("float64")
    df["notional"] = df["price"] * df["size"]
    df["event_ts"] = pd.to_datetime(df["event_ts_us"], unit="us", utc=True)
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts_us"], unit="us", utc=True)
    df["latency_ms"] = (df["ingest_ts_us"] - df["event_ts_us"]) / 1000.0
    for column in ("venue", "instrument_id", "side", "source"):
        df[column] = df[column].astype("category")

    ordered = [column for column in TRADE_COLUMNS if column in df.columns]
    rest = [column for column in df.columns if column not in ordered]
    return df[ordered + rest].sort_values("event_ts").reset_index(drop=True)


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse to one row per natural key, keeping the earliest arrival.

    Mirrors the Silver contract: `(venue, venue_symbol, trade_id)` is the
    identity of a trade, so a REST-repaired or archive-backfilled duplicate of
    a trade already delivered by the stream is the same trade, not a second one.

    PySpark equivalent: `dropDuplicates(NATURAL_KEY)` after ordering, or a
    `row_number()` window partitioned by the key when you need deterministic
    tie-breaking on `source`.
    """
    if df.empty:
        return df
    sort_columns = [c for c in ("event_ts", "kafka_offset") if c in df.columns]
    ordered = df.sort_values(sort_columns) if sort_columns else df
    return ordered.drop_duplicates(subset=NATURAL_KEY, keep="first").reset_index(drop=True)


def bars(
    df: pd.DataFrame,
    *,
    freq: str = "1min",
    by: str | list[str] = "instrument_id",
) -> pd.DataFrame:
    """OHLCV + VWAP bars on event time.

    Grouped by `instrument_id` alone by default, which makes a consolidated
    tape across venues. Pass `by=["venue", "instrument_id"]` for per-venue bars.

    Bars are built on `event_ts` (exchange time), never `ingest_ts` — using
    arrival time would let a slow consumer reshape the data. Dedupe first;
    duplicated trades inflate volume and VWAP.

    PySpark equivalent: `groupBy(window("event_ts", freq), *by)` with the same
    aggregations, and `sum(notional) / sum(size)` for VWAP.
    """
    keys = [by] if isinstance(by, str) else list(by)
    bar_columns = [
        *keys,
        "event_ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "notional",
        "trades",
        "vwap",
    ]
    if df.empty:
        return pd.DataFrame(columns=bar_columns)

    grouped = df.set_index("event_ts").groupby([*keys, pd.Grouper(freq=freq)], observed=True)
    out = grouped.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
        notional=("notional", "sum"),
        trades=("price", "count"),
    )
    out["vwap"] = out["notional"] / out["volume"]
    return out.reset_index()


def venue_comparison(df: pd.DataFrame, *, freq: str = "1min") -> pd.DataFrame:
    """Per-instrument VWAP per venue side by side, with the spread between them.

    The obvious first question once two venues carry the same instrument: do
    they agree, and by how much.
    """
    if df.empty:
        return pd.DataFrame()
    per_venue = bars(df, freq=freq, by=["venue", "instrument_id"])
    wide = per_venue.pivot_table(
        index=["instrument_id", "event_ts"], columns="venue", values="vwap", observed=True
    )
    venues = list(wide.columns)
    if len(venues) == 2:
        wide["spread"] = wide[venues[0]] - wide[venues[1]]
        wide["spread_bps"] = 10_000 * wide["spread"] / wide[venues[1]]
    return wide.reset_index()
