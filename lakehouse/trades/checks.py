"""Data-quality checks that assert design invariants rather than data ranges."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Run against the deployed table. Any row returned means SCD Type 1 has become
# lossy and the design must move to SCD Type 2 -- see the design doc §2.3.
#
# Only the immutable facts are compared. Provenance columns are deliberately
# absent: the stream and archive copies of one trade legitimately disagree on
# them, so including them would flag correct backfill as corruption.
IMMUTABILITY_SQL = """
SELECT venue,
       trade_id,
       COUNT(DISTINCT event_ts_us) AS distinct_event_ts,
       COUNT(DISTINCT price)       AS distinct_price,
       COUNT(DISTINCT size)        AS distinct_size
FROM fdai.market.silver_trades
GROUP BY venue, trade_id
HAVING distinct_event_ts > 1 OR distinct_price > 1 OR distinct_size > 1
"""


def find_immutability_violations(df: DataFrame) -> DataFrame:
    """One row per (venue, trade_id) whose immutable facts disagree.

    Mirrors IMMUTABILITY_SQL so the invariant can be tested locally against a
    synthetic frame as well as run against the deployed table.
    """
    return (
        df.groupBy("venue", "trade_id")
        .agg(
            F.countDistinct("event_ts_us").alias("distinct_event_ts"),
            F.countDistinct("price").alias("distinct_price"),
            F.countDistinct("size").alias("distinct_size"),
        )
        .where(
            (F.col("distinct_event_ts") > 1)
            | (F.col("distinct_price") > 1)
            | (F.col("distinct_size") > 1)
        )
    )
