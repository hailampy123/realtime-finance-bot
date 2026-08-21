from __future__ import annotations

from awsnative.athena import split_statements
from awsnative.monitoring.collect import _METRIC_COLUMNS, insert_statement, metric_data


def test_metric_data_skips_null_cells() -> None:
    rows = [{"table_name": "silver_trades", "row_count": "100", "quarantine_rate_pct": ""}]
    entries = metric_data(rows)
    names = {e["MetricName"] for e in entries}
    assert "RowCount" in names
    assert "QuarantineRatePct" not in names


def test_metric_data_dimensions_by_table_name() -> None:
    rows = [{"table_name": "gold_bars_1m", "row_count": "5"}]
    entries = metric_data(rows)
    assert entries[0]["Dimensions"] == [{"Name": "TableName", "Value": "gold_bars_1m"}]


def test_metric_data_uses_seconds_unit_for_seconds_columns() -> None:
    rows = [{"table_name": "silver_trades", "freshness_lag_seconds": "42"}]
    entries = metric_data(rows)
    assert entries[0]["Unit"] == "Seconds"


def test_metric_data_pascal_cases_metric_names() -> None:
    rows = [{"table_name": "silver_trades", "avg_file_size_mb": "1.5"}]
    entries = metric_data(rows)
    assert entries[0]["MetricName"] == "AvgFileSizeMb"


_FULL_ROW = {
    "metric_ts": "2026-08-19 12:00:00.000",
    "table_name": "silver_trades",
    "tier": "fast",
    **dict.fromkeys(_METRIC_COLUMNS, "1"),
}


def test_insert_statement_quotes_strings_not_numbers() -> None:
    sql = insert_statement("fdai_native", [_FULL_ROW])
    assert "'silver_trades'" in sql
    assert "'fast'" in sql
    assert "'2026-08-19 12:00:00.000'" in sql


def test_insert_statement_renders_null_for_empty_cell() -> None:
    row = {**_FULL_ROW, "quarantine_rate_pct": ""}
    sql = insert_statement("fdai_native", [row])
    assert "NULL" in sql


def test_insert_statement_is_one_statement_covering_every_row() -> None:
    sql = insert_statement("fdai_native", [_FULL_ROW, {**_FULL_ROW, "table_name": "gold_bars_1m"}])
    assert len(split_statements(sql)) == 1
    assert sql.count("'silver_trades'") == 1
    assert sql.count("'gold_bars_1m'") == 1
