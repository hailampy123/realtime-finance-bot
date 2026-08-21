-- One row of health metrics for one table, composed with others via
-- UNION ALL by render.py's health_metrics_select_statement() (spec
-- 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4.1).
--
-- freshness_expr and quarantine_expr are whole, already-resolved SQL
-- expressions, not bare column names: most tables have no meaningful
-- reading for one or the other (silver_trades_quarantine has no freshness
-- concept of its own; every table except silver_trades_quarantine has no
-- quarantine rate), so the caller renders CAST(NULL AS ...) for those and
-- splices the result in -- the same two-step composition
-- fragments/dirty_from_bronze.sql uses for dirty_cte.
--
-- delete_file_count assumes "$$files" exposes a `content` column
-- distinguishing delete files from data files (spec 2026-08-17 assumption
-- M2, unverified as of this writing -- see this plan's deploy task).
SELECT
    current_timestamp                                                     AS metric_ts,
    '${table}'                                                            AS table_name,
    '${tier}'                                                             AS tier,
    (SELECT count(*) FROM ${database}.${table})                          AS row_count,
    (SELECT count(*) FROM ${database}."${table}$$files")                 AS file_count,
    (SELECT avg(file_size_in_bytes) / 1e6 FROM ${database}."${table}$$files")
                                                                           AS avg_file_size_mb,
    (SELECT 100.0 * sum(CASE WHEN file_size_in_bytes < 100000000 THEN 1 ELSE 0 END) / count(*)
       FROM ${database}."${table}$$files")                               AS small_file_pct,
    (SELECT count(*) FROM ${database}."${table}$$files" WHERE content <> 0)
                                                                           AS delete_file_count,
    (SELECT count(*) FROM ${database}."${table}$$snapshots")             AS snapshot_count,
    (SELECT to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
       FROM ${database}."${table}$$snapshots")                           AS oldest_snapshot_age_seconds,
    ${freshness_expr}                                                    AS freshness_lag_seconds,
    ${quarantine_expr}                                                   AS quarantine_rate_pct
