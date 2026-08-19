-- Silver: macro series, one row per distinct value per observation per vintage.
--
-- WHY vintage_date IS PART OF THE KEY. Macro data is revised. Verified against
-- real ALFRED responses: August 2025 CPI read 323.364 in the January 2026 vintage
-- and 323.291 in the April 2026 vintage, and 59 of 947 overlapping observations
-- changed across that boundary. A revision is therefore a NEW row rather than an
-- update, and the merge is insert-only so the history cannot be destroyed. An
-- UPDATE branch here would delete exactly what the point-in-time join reads.
--
-- knowledge_ts_us IS THE MIDNIGHT AFTER THE VINTAGE, not the vintage's own
-- midnight. A vintage published at some hour of its day is safely knowable by the
-- following midnight; claiming the day's own start would assert knowledge up to
-- 24 hours early, which is a lookahead leak of exactly the kind this column
-- exists to prevent. The cost is up to a day of conservatism, which is the right
-- direction to be wrong in.
--
-- observation_date IS NOT A KNOWLEDGE BOUNDARY and must never be filtered on by a
-- read path. A CPI observation is stamped with the month it measures and is
-- published about six weeks later. Joining on it lets a backtest standing on
-- 2025-09-01 read a number that did not exist until February 2026.
--
-- PARTITIONED BY series_id only. Six series and a few thousand rows: partitioning
-- by date as well would produce thousands of partitions holding one row each,
-- which costs more in metadata than it saves in pruning.
CREATE TABLE IF NOT EXISTS ${database}.silver_macro (
    series_id        string,
    observation_date date,
    vintage_date     date,
    value            decimal(38, 18),
    knowledge_ts_us  bigint,
    source_tier      string
)
PARTITIONED BY (series_id)
LOCATION '${warehouse}silver_macro/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
