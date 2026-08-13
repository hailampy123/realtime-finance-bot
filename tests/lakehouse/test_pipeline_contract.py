"""Offline contract tests for the pipeline shell.

The shell imports `pyspark.pipelines`, which exists only on Databricks Runtime,
so it cannot be imported here. Parsing it instead still pins the values whose
drift would be invisible until it corrupted data: the CDC keys, the sequence
column, the SCD type, the flow name, and Change Data Feed.
"""

from __future__ import annotations

import ast
from pathlib import Path

PIPELINE = Path("lakehouse/pipelines/trades.py")


def _tree() -> ast.Module:
    return ast.parse(PIPELINE.read_text(encoding="utf-8"))


def _call(name: str) -> ast.Call:
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Call):
            func = node.func
            attr = func.attr if isinstance(func, ast.Attribute) else None
            if attr == name:
                return node
    raise AssertionError(f"no call to {name} found in {PIPELINE}")


def _kwarg(call: ast.Call, name: str) -> object:
    for kw in call.keywords:
        if kw.arg == name:
            return ast.literal_eval(kw.value)
    raise AssertionError(f"{name} not passed")


def test_pipeline_file_exists():
    assert PIPELINE.exists()


def test_cdc_flow_pins_the_dedupe_contract():
    call = _call("create_auto_cdc_flow")
    # Renaming the flow would orphan its checkpoint; changing keys or the
    # sequence column would silently change which duplicate wins.
    assert _kwarg(call, "name") == "cdc_trades_stream"
    assert _kwarg(call, "target") == "silver_trades"
    assert _kwarg(call, "keys") == ["venue", "trade_id"]
    assert _kwarg(call, "sequence_by") == "event_ts_us"
    assert _kwarg(call, "stored_as_scd_type") == 1


def test_silver_enables_change_data_feed():
    # Stage 2b's scoped recompute reads CDF. Enabling it after the table has
    # data does not produce change data for existing commits.
    props = _kwarg(_call("create_streaming_table"), "table_properties")
    assert isinstance(props, dict)
    assert props["delta.enableChangeDataFeed"] == "true"


def test_cdc_flow_has_no_except_column_list():
    # Regression test for a live failure on 2026-08-13. trades_clean (the
    # flow's source) is built by transforms.valid_trades, which already
    # projects away every Kafka audit column before this flow ever runs.
    # except_column_list must resolve its names against the source schema, and
    # none of them exist there any more, so declaring it failed the flow with
    # UNRESOLVED_COLUMN before a single record was processed. The exclusion is
    # enforced entirely by the projection -- see
    # tests/lakehouse/test_silver.py::test_silver_drops_kafka_metadata.
    call = _call("create_auto_cdc_flow")
    assert not any(kw.arg == "except_column_list" for kw in call.keywords)


def test_shell_holds_no_business_logic():
    # Any predicate or cast here would be untested, since this file cannot run
    # under pytest. Logic belongs in transforms.py.
    source = PIPELINE.read_text(encoding="utf-8")
    for banned in ("try_cast", "EPOCH_FLOOR_US", "DECIMAL(38,18)", "timestamp_micros"):
        assert banned not in source, f"{banned} belongs in transforms.py"


def test_session_timezone_is_pinned_to_utc():
    # Databricks defaults to UTC, but relying on a default means a workspace
    # setting could silently shift every event_ts. See the conftest note.
    source = PIPELINE.read_text(encoding="utf-8")
    assert "spark.sql.session.timeZone" in source
