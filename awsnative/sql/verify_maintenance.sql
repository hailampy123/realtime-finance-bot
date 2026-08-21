-- Maintenance health, per maintained table. Run after a housekeeping pass to
-- confirm OPTIMIZE and VACUUM did what section 8.4 of
-- 2026-08-17-iceberg-table-maintenance-design.md asks for: file count and
-- average file size trending down after OPTIMIZE, snapshot count bounded
-- after VACUUM. Numbers, not a pass/fail: read them next to the numbers from
-- before the pass ran.

-- 1. File count and average size, active snapshot only.
SELECT 'silver_trades' AS table_name, count(*) AS file_count,
       avg(file_size_in_bytes) / 1e6 AS avg_file_size_mb
FROM "silver_trades$files"
UNION ALL
SELECT 'silver_trades_quarantine', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_trades_quarantine$files"
UNION ALL
SELECT 'gold_bars_1m', count(*), avg(file_size_in_bytes) / 1e6
FROM "gold_bars_1m$files"
UNION ALL
SELECT 'silver_perp_context', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_perp_context$files"
UNION ALL
SELECT 'silver_macro', count(*), avg(file_size_in_bytes) / 1e6
FROM "silver_macro$files"
ORDER BY table_name;

-- 2. Snapshot count and the oldest snapshot's age. A count that only grows
--    means VACUUM is not running or is not committing.
SELECT 'silver_trades' AS table_name, count(*) AS snapshot_count,
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at)) AS oldest_snapshot_age_seconds
FROM "silver_trades$snapshots"
UNION ALL
SELECT 'silver_trades_quarantine', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_trades_quarantine$snapshots"
UNION ALL
SELECT 'gold_bars_1m', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "gold_bars_1m$snapshots"
UNION ALL
SELECT 'silver_perp_context', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_perp_context$snapshots"
UNION ALL
SELECT 'silver_macro', count(*),
       to_unixtime(current_timestamp) - to_unixtime(min(committed_at))
FROM "silver_macro$snapshots"
ORDER BY table_name;
