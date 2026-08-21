-- Health metrics: one row per (table, tick), written by the CollectHealthMetrics
-- tail state in three different state machines (spec
-- 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4). The only
-- Iceberg table in this repo with more than one writer -- see the plain
-- INSERT (not MERGE) in awsnative/monitoring/collect.py for why that is safe
-- here specifically.
--
-- Partitioned on day(metric_ts) alone, like silver_trades on day(event_ts):
-- the partition value is derived from the timestamp column, so there is no
-- separate stored date column to keep in sync with it.
CREATE TABLE IF NOT EXISTS ${database}.native_health_metrics (
    metric_ts                   timestamp,
    table_name                  string,
    tier                        string,
    row_count                   bigint,
    file_count                  bigint,
    avg_file_size_mb            double,
    small_file_pct              double,
    delete_file_count           bigint,
    snapshot_count              bigint,
    oldest_snapshot_age_seconds bigint,
    freshness_lag_seconds       bigint,
    quarantine_rate_pct         double
)
PARTITIONED BY (day(metric_ts))
LOCATION '${warehouse}native_health_metrics/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy',
    'vacuum_max_snapshot_age_seconds' = '3600',
    'vacuum_min_snapshots_to_keep'    = '5'
)
