-- How far behind now each table's newest row is. The "is it still running" check.
--
-- Seconds, not a formatted string: the page decides the status band, so the
-- thresholds live in one place in Python rather than being baked into SQL here
-- and re-stated in the renderer.
--
-- A table with no rows returns NULL rather than a huge number. Absent and stale
-- are different conditions with different causes, and collapsing them would make
-- a never-started pipeline look like a lagging one.
SELECT 'silver_trades' AS table_name,
       to_unixtime(current_timestamp) - to_unixtime(max(event_ts)) AS lag_seconds,
       count(*) AS row_count
FROM ${database}.silver_trades
UNION ALL
SELECT 'gold_bars_1m',
       to_unixtime(current_timestamp) - to_unixtime(max(window_end_ts)),
       count(*)
FROM ${database}.gold_bars_1m
UNION ALL
SELECT 'silver_perp_context',
       to_unixtime(current_timestamp) - to_unixtime(max(snapshot_ts)),
       count(*)
FROM ${database}.silver_perp_context
UNION ALL
SELECT 'silver_macro',
       to_unixtime(current_timestamp) - to_unixtime(CAST(max(vintage_date) AS TIMESTAMP)),
       count(*)
FROM ${database}.silver_macro
