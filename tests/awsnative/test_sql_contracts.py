"""Contract tests between the Python that describes the tables and the DDL that
creates them.

Neither can check the other at runtime -- one is a module, the other is text
sent to Athena -- so the agreement has to be asserted here or not at all.
"""

from __future__ import annotations

import re

from awsnative import render
from awsnative.bars import ADDITIVE_EXTREMA, ADDITIVE_SUM, NON_ADDITIVE

_COLUMN = re.compile(
    r"^\s+`?(\w+)`?\s+(string|timestamp|bigint|decimal|double|int|boolean)\b",
    re.MULTILINE,
)

DDL = dict(render.ddl_statements("fdai_native", "s3://bucket/"))
SILVER = DDL["010_silver_trades.sql"]
QUARANTINE = DDL["020_silver_trades_quarantine.sql"]
GOLD = DDL["030_gold_bars_1m.sql"]


def columns(ddl: str) -> set[str]:
    return {match.group(1) for match in _COLUMN.finditer(ddl)}


def test_gold_declares_every_component_bars_py_names() -> None:
    """The drift tripwire. Add a measure to bars.py without adding the column
    and this fails; add the column without the measure and test_bars.py never
    exercises it."""
    declared = columns(GOLD)
    for name in (*ADDITIVE_SUM, *ADDITIVE_EXTREMA, *NON_ADDITIVE):
        assert name in declared, f"gold_bars_1m does not declare {name}"


def test_gold_stores_no_precomputed_ratio() -> None:
    """A vwap or imbalance column would be correct at exactly one grain and
    quietly wrong at every other (spec 5.3)."""
    declared = columns(GOLD)
    for forbidden in ("vwap", "flow_imbalance", "imbalance", "realized_vol"):
        assert forbidden not in declared, f"gold_bars_1m stores the ratio {forbidden}"


def test_gold_carries_the_fidelity_marker() -> None:
    """Without source_tier a two-year backtest silently mixes trade-derived and
    kline-derived bars, and the model appears to improve over time when all
    that improved is the input data (spec 6.4)."""
    assert "source_tier" in columns(GOLD)


def test_silver_partitioning_matches_the_dirty_partition_unit() -> None:
    """(instrument_id, day) is what makes "a backfill of one symbol-day rebuilds
    one symbol-day" true, and it is what the Gold merge joins on."""
    assert "PARTITIONED BY (instrument_id, day(event_ts))" in SILVER
    assert "PARTITIONED BY (instrument_id, day(window_end_ts))" in GOLD


def test_every_medallion_table_is_iceberg() -> None:
    """Bronze is deliberately plain Parquet (D1); everything downstream is a
    MERGE target and therefore is not."""
    for name, ddl in DDL.items():
        assert "'table_type'        = 'ICEBERG'" in ddl, f"{name} is not an Iceberg table"


def test_quarantine_keeps_price_and_size_as_strings() -> None:
    """A row lands in quarantine precisely because a value would not cast, so a
    DECIMAL column here could not hold the thing you came to look at."""
    assert re.search(r"^\s+price\s+string", QUARANTINE, re.MULTILINE)
    assert re.search(r"^\s+`size`\s+string", QUARANTINE, re.MULTILINE)
    # Silver, having only cast-able rows, does not have that problem.
    assert re.search(r"^\s+price\s+decimal", SILVER, re.MULTILINE)


def test_quarantine_is_keyed_on_a_null_safe_hash() -> None:
    """A row can be quarantined FOR having a NULL venue, and NULL = NULL is
    never true -- so keying on (venue, trade_id) would re-insert those rows on
    every five-minute run, forever."""
    assert "row_key" in columns(QUARANTINE)
    quarantine_merge = render.merge_statements("fdai_native")[render.MERGE_QUARANTINE]
    assert "ON t.row_key = s.row_key" in quarantine_merge
    assert "md5(to_utf8(" in quarantine_merge


def test_silver_keeps_both_the_exact_and_the_convenient_timestamp() -> None:
    """from_unixtime truncates to milliseconds, so event_ts alone would quietly
    lose the microseconds the wire format carries."""
    declared = columns(SILVER)
    assert {"event_ts", "event_ts_us", "ingest_ts", "ingest_ts_us"} <= declared


def test_silver_columns_cover_the_bronze_contract() -> None:
    """Every field the encoder writes, minus schema_version, which describes the
    envelope rather than the trade and stops being useful once parsed."""
    declared = columns(SILVER)
    expected = {
        "venue",
        "venue_symbol",
        "instrument_id",
        "trade_id",
        "event_ts_us",
        "ingest_ts_us",
        "price",
        "size",
        "side",
        "sequence",
        "is_backfill",
        "source",
    }
    assert expected <= declared
