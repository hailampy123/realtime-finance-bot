from __future__ import annotations

from typing import Any

import pytest

from awsnative.athena import AthenaError, AthenaRunner, split_statements


class FakeAthena:
    """Replays a scripted sequence of states for one query."""

    def __init__(self, states: list[str], scanned: int = 1024) -> None:
        self._states = states
        self._scanned = scanned
        self.started: list[dict[str, Any]] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.started.append(kwargs)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, *, QueryExecutionId: str) -> dict[str, Any]:
        state = self._states.pop(0) if len(self._states) > 1 else self._states[0]
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": "because"},
                "Statistics": {
                    "DataScannedInBytes": self._scanned,
                    "TotalExecutionTimeInMillis": 42,
                },
            }
        }

    def get_query_results(self, *, QueryExecutionId: str, MaxResults: int) -> dict[str, Any]:
        return {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "venue"}, {"VarCharValue": "rows"}]},
                    {"Data": [{"VarCharValue": "binance"}, {"VarCharValue": "7"}]},
                ]
            }
        }


def make_runner(fake: FakeAthena, **kwargs: Any) -> AthenaRunner:
    return AthenaRunner(
        database="fdai_native",
        workgroup="fdai-native",
        client=fake,
        sleep=lambda _: None,
        poll_interval=0.0,
        **kwargs,
    )


def test_polls_until_terminal_and_reports_bytes_scanned() -> None:
    """Data scanned is the number that says whether partition pruning worked,
    and the number the bill comes from. Losing it makes cheap and expensive
    queries look identical."""
    fake = FakeAthena(["QUEUED", "RUNNING", "SUCCEEDED"], scanned=2_097_152)
    outcome = make_runner(fake).execute("SELECT 1")

    assert outcome.ok
    assert outcome.data_scanned_mb == pytest.approx(2.0)
    assert outcome.elapsed_ms == 42


def test_a_failed_query_raises_with_the_reason() -> None:
    fake = FakeAthena(["FAILED"])
    with pytest.raises(AthenaError, match="because"):
        make_runner(fake).execute("SELECT 1")


def test_start_and_wait_reports_failure_without_raising() -> None:
    """ddl.py needs the reason text to tell AlreadyExists from a real error."""
    fake = FakeAthena(["FAILED"])
    outcome = make_runner(fake).start_and_wait("SELECT 1")
    assert not outcome.ok
    assert outcome.reason == "because"


def test_timeout_does_not_cancel_the_query() -> None:
    """Iceberg commits are atomic, so a half-run merge is not half-applied --
    but cancelling would hide a slow query behind a timeout that reads like a
    different bug."""
    fake = FakeAthena(["RUNNING"])
    with pytest.raises(AthenaError, match="still RUNNING"):
        make_runner(fake, timeout=0.0).execute("SELECT 1")


def test_trailing_semicolon_is_stripped_before_sending() -> None:
    fake = FakeAthena(["SUCCEEDED"])
    make_runner(fake).execute("SELECT 1;\n")
    assert fake.started[0]["QueryString"] == "SELECT 1"


def test_fetch_rows_zips_the_header_row() -> None:
    fake = FakeAthena(["SUCCEEDED"])
    assert make_runner(fake).fetch_rows("q-1") == [{"venue": "binance", "rows": "7"}]


# --- split_statements ------------------------------------------------------


def test_splits_on_semicolons_and_drops_comments() -> None:
    text = "-- a header\nSELECT 1;\n-- another\nSELECT 2;\n"
    assert split_statements(text) == ["SELECT 1", "SELECT 2"]


def test_a_semicolon_inside_a_string_does_not_split() -> None:
    """The reason this is not text.split(';'): the naive version truncates the
    statement rather than failing, so the query runs and returns wrong rows."""
    text = "SELECT concat_ws(';', a, b) FROM t;"
    assert split_statements(text) == ["SELECT concat_ws(';', a, b) FROM t"]


def test_a_semicolon_inside_a_comment_does_not_split() -> None:
    text = "SELECT 1 -- ends with ; here\nFROM t;"
    assert split_statements(text) == ["SELECT 1 \nFROM t"]


def test_a_double_dash_inside_a_string_is_not_a_comment() -> None:
    text = "SELECT '--not a comment' AS x;"
    assert split_statements(text) == ["SELECT '--not a comment' AS x"]


def test_trailing_statement_without_a_semicolon_is_kept() -> None:
    assert split_statements("SELECT 1") == ["SELECT 1"]


def test_comment_only_input_yields_nothing() -> None:
    assert split_statements("-- just a note\n\n") == []


def test_the_verification_file_splits_into_its_documented_queries() -> None:
    """verify_silver_gold.sql is numbered 1..7 in its own comments; if the
    splitter disagrees with the numbering, one of them is wrong."""
    from awsnative.render import SQL_DIR

    statements = split_statements((SQL_DIR / "verify_silver_gold.sql").read_text())
    assert len(statements) == 7
    assert all(s.upper().startswith(("SELECT", "WITH")) for s in statements)
