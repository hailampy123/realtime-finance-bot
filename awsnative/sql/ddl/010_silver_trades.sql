-- Silver: the deduplicated trade fact table.
--
-- Iceberg, not plain Parquet, because this is a MERGE target -- the one thing
-- Bronze never is (spec D1/D2). Iceberg on plain S3, not S3 Tables: S3 Tables
-- sells managed compaction and snapshot expiry, and the weekly wipe means no
-- table lives long enough to need either.
--
-- Column notes worth knowing before you query this:
--
--   event_ts_us / ingest_ts_us  are the AUTHORITATIVE instants, carried through
--       from the wire format unchanged. Compare on these when precision matters.
--   event_ts / ingest_ts        are millisecond-precision conveniences derived
--       from them, because Trino's from_unixtime() truncates to milliseconds.
--       They exist so partitioning and BETWEEN predicates read naturally. They
--       are never more precise than the _us columns and never disagree with
--       them by more than 1 ms.
--   price / size                become DECIMAL here, having been strings in
--       Bronze. A value that will not cast never reaches this table -- it is
--       quarantined -- which is why the cast is safe without try_cast.
--
-- PARTITIONED BY (instrument_id, day(event_ts)) is why this DDL cannot be a
-- Terraform aws_glue_catalog_table: Glue's CreateTable API accepts identity
-- partitions only and cannot express the day() transform at all.
--
-- (instrument_id, day) is also exactly the dirty-partition unit the Gold
-- rebuild works in, which is not a coincidence -- it is what makes "a backfill
-- of one symbol-day rebuilds one symbol-day" true.
CREATE TABLE IF NOT EXISTS ${database}.silver_trades (
    venue         string,
    venue_symbol  string,
    instrument_id string,
    trade_id      string,
    event_ts      timestamp,
    ingest_ts     timestamp,
    event_ts_us   bigint,
    ingest_ts_us  bigint,
    price         decimal(38, 18),
    `size`        decimal(38, 18),
    side          string,
    sequence      bigint,
    is_backfill   boolean,
    source        string
)
PARTITIONED BY (instrument_id, day(event_ts))
LOCATION '${warehouse}silver_trades/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy',
    'vacuum_max_snapshot_age_seconds' = '3600',
    'vacuum_min_snapshots_to_keep'    = '5'
)
