"""Guards the claim that makes SCD Type 1 safe (design §2.3 / parent §4.2).

If a trade's facts can change under a fixed (venue, trade_id), then a keyed
upsert silently discards one version and the design must move to SCD Type 2.
This test is the tripwire for that.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

pytest.importorskip("pyspark", reason="needs `uv sync --group lakehouse`")

from lakehouse.trades.checks import IMMUTABILITY_SQL, find_immutability_violations

_SCHEMA = (
    "venue string, trade_id string, event_ts_us long, price decimal(38,18), size decimal(38,18)"
)


def test_identical_duplicates_are_not_violations(spark):
    # The stream copy and the archive copy of one trade: same facts, different
    # provenance. This must be accepted, or backfill would look like corruption.
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
    ]
    assert find_immutability_violations(spark.createDataFrame(rows, _SCHEMA)).count() == 0


def test_differing_price_for_one_key_is_a_violation(spark):
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_000, Decimal("999.0"), Decimal("0.25")),
    ]
    violations = find_immutability_violations(spark.createDataFrame(rows, _SCHEMA)).collect()
    assert len(violations) == 1
    assert violations[0]["trade_id"] == "1"


def test_differing_timestamp_for_one_key_is_a_violation(spark):
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_001, Decimal("100.5"), Decimal("0.25")),
    ]
    assert find_immutability_violations(spark.createDataFrame(rows, _SCHEMA)).count() == 1


def test_differing_size_for_one_key_is_a_violation(spark):
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.99")),
    ]
    assert find_immutability_violations(spark.createDataFrame(rows, _SCHEMA)).count() == 1


def test_same_trade_id_on_different_venues_is_not_a_violation(spark):
    # trade_id is only unique per venue, which is why the key is a pair.
    rows = [
        ("binance", "1", 1_700_000_000_000_000, Decimal("100.5"), Decimal("0.25")),
        ("coinbase", "1", 1_700_000_000_000_009, Decimal("101.0"), Decimal("0.30")),
    ]
    assert find_immutability_violations(spark.createDataFrame(rows, _SCHEMA)).count() == 0


def test_sql_form_targets_the_silver_table():
    assert "fdai.market.silver_trades" in IMMUTABILITY_SQL
    assert "GROUP BY" in IMMUTABILITY_SQL.upper()


def test_sql_form_ignores_provenance_columns(spark):
    # source and is_backfill legitimately differ between the stream and archive
    # copies of a trade, so including them would flag correct backfill.
    assert "source" not in IMMUTABILITY_SQL
    assert "is_backfill" not in IMMUTABILITY_SQL
