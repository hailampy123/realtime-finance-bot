"""Publish per-table health metrics to CloudWatch and append them to
native_health_metrics.

WHY A LAMBDA HERE, narrowing awsnative/athena.py's own stated preference
("Step Functions calls Athena directly... that is the whole reason stage N2
needs no Lambda"). That reasoning is about the write-critical merge path:
Bronze -> Silver -> Gold, where an extra hop must not add latency or a new
failure mode to data the pipeline exists to produce. CollectHealthMetrics
runs strictly after the merge and its maintenance pass already committed --
it only reads Iceberg metadata and publishes numbers, so a Lambda failing
here cannot corrupt or delay the tables it reports on. In exchange, the
row-to-metric mapping gets real pytest coverage instead of living only in
Step Functions JSON at fixed array positions, where a reordered SELECT
column would silently point a CloudWatch alarm at the wrong number until
someone noticed live.

WHY INSERT, NOT MERGE, into native_health_metrics. This is the only Iceberg
table in this repo with more than one writer: the microbatch, the perp
merge, and the macro merge all append to it. Iceberg's optimistic commit
lock is table-level, not partition-level, so two of those writers landing at
the same moment is a real (if rare) collision -- something no other table
here has had to consider. A MERGE keyed on (table_name, metric_ts) would
still not help, because metric_ts comes from current_timestamp evaluated
inside the query: a retried statement gets a new timestamp, never matches
its own failed attempt, and would insert anyway. Given that, a plain INSERT
is not a shortcut, it is the honest statement of what actually happens on
retry -- and the cost of a rare extra row in an observability table is a
cosmetic blip, not a wrong answer, unlike a duplicated trade or bar would be.
"""

from __future__ import annotations

from typing import Any

_METRIC_COLUMNS = (
    "row_count",
    "file_count",
    "avg_file_size_mb",
    "small_file_pct",
    "delete_file_count",
    "snapshot_count",
    "oldest_snapshot_age_seconds",
    "freshness_lag_seconds",
    "quarantine_rate_pct",
)


def _f(value: str | None) -> float | None:
    """Athena returns every cell as text, and an empty cell means NULL."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _pascal_case(column: str) -> str:
    """row_count -> RowCount, matching spec 2026-08-19 section 4.2's metric names."""
    return "".join(part.capitalize() for part in column.split("_"))


def metric_data(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """CloudWatch PutMetricData entries, one per (table, KPI) that has a value.

    A NULL cell (Athena's empty string) is skipped rather than published as
    zero: "no quarantine rate for this table" and "a measured rate of zero"
    are different facts, and publishing the first as the second would make a
    healthy table indistinguishable from one this metric does not apply to.
    """
    entries: list[dict[str, Any]] = []
    for row in rows:
        table = row["table_name"]
        for column in _METRIC_COLUMNS:
            value = _f(row.get(column))
            if value is None:
                continue
            entries.append(
                {
                    "MetricName": _pascal_case(column),
                    "Dimensions": [{"Name": "TableName", "Value": table}],
                    "Value": value,
                    "Unit": "Seconds" if column.endswith("_seconds") else "None",
                }
            )
    return entries


def _sql_literal(value: str | None, *, quote: bool) -> str:
    """One already-fetched cell, rendered back into a SQL literal.

    Only ever called on values Athena itself just produced from COUNT, AVG,
    or a fixed table-name/tier string -- never on external input -- so a
    plain quote is enough; there is no untrusted text to escape.
    """
    if value in (None, ""):
        return "NULL"
    return f"'{value}'" if quote else value


_INSERT_COLUMNS = ("metric_ts", "table_name", "tier", *_METRIC_COLUMNS)
_STRING_COLUMNS = frozenset({"metric_ts", "table_name", "tier"})


def insert_statement(database: str, rows: list[dict[str, str]]) -> str:
    """One INSERT covering every row already fetched for CloudWatch.

    Built from the values already read back from Athena, rather than
    re-running the SELECT a second time -- current_timestamp evaluated twice,
    moments apart, would make the timestamp CloudWatch published disagree
    with the one stored in history.
    """
    value_rows = [
        "({})".format(
            ", ".join(
                _sql_literal(row.get(column), quote=column in _STRING_COLUMNS)
                for column in _INSERT_COLUMNS
            )
        )
        for row in rows
    ]
    columns_sql = ", ".join(_INSERT_COLUMNS)
    return (
        f"INSERT INTO {database}.native_health_metrics ({columns_sql}) "
        f"VALUES {', '.join(value_rows)}"
    )


def health_metrics_handler(event: dict[str, Any], context: object = None) -> dict[str, str]:
    """Lambda entry point, one call per tail state.

    Event: {"database": "...", "workgroup": "...", "tables": ["silver_trades", ...]}
    """
    from awsnative.athena import AthenaRunner
    from awsnative.render import health_metrics_select_statement

    database = event["database"]
    tables = event["tables"]
    runner = AthenaRunner(database=database, workgroup=event["workgroup"])

    select_sql = health_metrics_select_statement(database, tables)
    outcome = runner.execute(select_sql)
    rows = runner.fetch_rows(outcome.query_execution_id, max_rows=len(tables))

    import boto3

    entries = metric_data(rows)
    if entries:
        # 1000 entries per call is PutMetricData's own limit, comfortably
        # above what 9 KPIs times at most 3 tables per invocation produces.
        boto3.client("cloudwatch").put_metric_data(Namespace="FDAI/Native", MetricData=entries)

    runner.execute(insert_statement(database, rows))
    return {"tables": str(len(rows))}
