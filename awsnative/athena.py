"""A minimal synchronous Athena client.

Athena is asynchronous: StartQueryExecution returns an id, and you poll. That
is three API calls and a loop every time, which is enough friction to make
people paste SQL into the console instead -- so it lives here once.

This is NOT what runs the micro-batch. Step Functions calls Athena directly
through its .sync service integration, with no code in between; that is the
whole reason stage N2 needs no Lambda. This module is for the DDL, for the
verification queries, and for running a merge by hand while debugging.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

_TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


@dataclass(frozen=True)
class QueryOutcome:
    """What a finished query cost and whether it worked."""

    query_execution_id: str
    state: str
    data_scanned_bytes: int
    elapsed_ms: int
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.state == "SUCCEEDED"

    @property
    def data_scanned_mb(self) -> float:
        return self.data_scanned_bytes / 1_048_576


class AthenaError(RuntimeError):
    """A query reached a terminal state that was not SUCCEEDED."""


def split_statements(text: str) -> list[str]:
    """Split a .sql file into individual statements.

    Quote- and comment-aware, because the naive text.split(';') is wrong the
    first time a string literal or a comment contains a semicolon, and wrong
    silently -- it truncates a statement rather than failing.

    Comments are stripped rather than sent: Athena accepts them, but they make
    the console's query list unreadable when every entry starts with forty
    lines of rationale.
    """
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_line_comment = False

    chars = iter(_with_lookahead(text))
    for char, next_char in chars:
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                current.append(char)
            continue

        if in_single_quote:
            current.append(char)
            if char == "'":
                in_single_quote = False
            continue

        if char == "'":
            in_single_quote = True
            current.append(char)
        elif char == "-" and next_char == "-":
            in_line_comment = True
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)

    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def _with_lookahead(text: str) -> Iterator[tuple[str, str]]:
    for index, char in enumerate(text):
        yield char, text[index + 1] if index + 1 < len(text) else ""


class AthenaRunner:
    def __init__(
        self,
        *,
        database: str,
        workgroup: str,
        client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        poll_interval: float = 2.0,
        timeout: float = 900.0,
    ) -> None:
        self._database = database
        self._workgroup = workgroup
        self._client = client if client is not None else _default_client()
        self._sleep = sleep
        self._poll_interval = poll_interval
        self._timeout = timeout

    def execute(self, sql: str) -> QueryOutcome:
        """Run one statement to completion. Raises AthenaError unless it succeeded."""
        outcome = self.start_and_wait(sql)
        if not outcome.ok:
            raise AthenaError(
                f"query {outcome.query_execution_id} ended {outcome.state}: {outcome.reason}"
            )
        return outcome

    def start_and_wait(self, sql: str) -> QueryOutcome:
        """Run one statement to completion and report the outcome, success or not."""
        started = self._client.start_query_execution(
            QueryString=sql.rstrip().rstrip(";"),
            QueryExecutionContext={"Database": self._database},
            WorkGroup=self._workgroup,
        )
        execution_id = started["QueryExecutionId"]

        waited = 0.0
        while True:
            described = self._client.get_query_execution(QueryExecutionId=execution_id)
            execution = described["QueryExecution"]
            status = execution["Status"]
            state = status["State"]
            if state in _TERMINAL:
                statistics = execution.get("Statistics", {})
                return QueryOutcome(
                    query_execution_id=execution_id,
                    state=state,
                    data_scanned_bytes=int(statistics.get("DataScannedInBytes", 0) or 0),
                    elapsed_ms=int(statistics.get("TotalExecutionTimeInMillis", 0) or 0),
                    reason=status.get("StateChangeReason"),
                )
            if waited >= self._timeout:
                # Leave the query running rather than cancelling it: a merge
                # killed halfway is not partially applied (Iceberg commits are
                # atomic), but cancelling hides a slow query behind a timeout
                # that looks like a different bug.
                raise AthenaError(f"query {execution_id} still {state} after {self._timeout:.0f}s")
            self._sleep(self._poll_interval)
            waited += self._poll_interval

    def fetch_rows(self, query_execution_id: str, max_rows: int = 100) -> list[dict[str, str]]:
        """The first page of results, as dicts. Not paginated on purpose.

        The verification queries all LIMIT themselves; anything needing a second
        page wants the console or a CTAS, not this.
        """
        response = self._client.get_query_results(
            QueryExecutionId=query_execution_id, MaxResults=min(max_rows + 1, 1000)
        )
        rows = response["ResultSet"]["Rows"]
        if not rows:
            return []
        header = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]
        return [
            dict(zip(header, (cell.get("VarCharValue", "") for cell in row["Data"]), strict=False))
            for row in rows[1:]
        ]


def _default_client() -> Any:
    import boto3

    return boto3.client("athena")
