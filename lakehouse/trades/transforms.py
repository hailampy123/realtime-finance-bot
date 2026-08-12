"""Pure PySpark transforms for the trades Bronze/Silver path.

Every function here takes and returns a DataFrame and touches no Databricks-only
API, which is what lets the whole path be tested on a laptop. The declarative
pipeline in lakehouse/pipelines/trades.py is a thin shell over these.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from lakehouse.trades.schema import trade_avsc_json

# The producer writes a bare Avro datum via fastavro.schemaless_writer, with no
# Confluent magic-byte prefix and no schema registry, which is exactly what
# from_avro's jsonFormatSchema form consumes. See ingest/core/codec.py.
#
# PERMISSIVE, not the default FAILFAST: FAILFAST aborts the entire micro-batch
# on a single malformed record, so one poison message halts ingestion forever.
# PERMISSIVE yields a NULL struct, which classify_trades turns into a
# quarantined row.
_AVRO_OPTIONS = {"mode": "PERMISSIVE"}

_DECODED = "_decoded"


def decode_kafka_trades(df: DataFrame) -> DataFrame:
    """Decode Kafka's binary `value` into flat trade columns, keeping audit data."""
    return df.withColumn(
        _DECODED, from_avro(F.col("value"), trade_avsc_json(), _AVRO_OPTIONS)
    ).select(
        F.col(f"{_DECODED}.*"),
        F.col("value").alias("_kafka_value"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )
