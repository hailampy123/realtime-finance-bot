-- Quarantine: rows the validity contract rejected. Never dropped (spec 5.4) --
-- a silent discard destroys the ability to explain a gap months later, when the
-- Bronze partition that held the evidence has already been wiped.
--
-- Every payload column is the RAW Bronze type, string for price and size
-- included. That is the point: a row lands here precisely because a value would
-- not cast, so a DECIMAL column here could not store the thing you came to look
-- at.
--
-- row_key is a synthetic natural key, not decoration. The merge into this table
-- must be idempotent, because the micro-batch re-reads an overlapping Bronze
-- window every five minutes and a plain INSERT would re-add the same bad row
-- 288 times a day. Keying on (venue, trade_id) does not work here: a row can be
-- quarantined *for* having a NULL venue, and NULL = NULL is never true, so
-- those rows would duplicate forever. Hashing the whole raw tuple is NULL-safe
-- and keeps genuinely distinct broken rows distinct.
--
-- quarantine_reason is diagnostic and best-effort. fragments/valid_trade.sql is
-- authoritative about WHETHER a row is quarantined; this column is a hint about
-- why, and reads 'unknown' if the two ever disagree.
CREATE TABLE IF NOT EXISTS ${database}.silver_trades_quarantine (
    row_key           string,
    venue             string,
    venue_symbol      string,
    instrument_id     string,
    trade_id          string,
    event_ts_us       bigint,
    ingest_ts_us      bigint,
    price             string,
    `size`            string,
    side              string,
    sequence          bigint,
    is_backfill       boolean,
    source            string,
    ingest_date       string,
    quarantine_reason string,
    quarantined_ts    timestamp
)
PARTITIONED BY (ingest_date)
LOCATION '${warehouse}silver_trades_quarantine/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
