-- Deep-tier staging: 1-minute klines as they came out of the archive.
--
-- EXTERNAL, gzipped CSV, and every column a string. Three deliberate choices:
--
-- CSV rather than Parquet narrows spec §6.2's "write Parquet" while keeping the
-- requirement stated in the same paragraph, which is the load-bearing one:
-- "Lambda does I/O only ... All merging is Athena SQL against a staging external
-- table. That keeps exactly one transform engine." Parquet from Lambda means
-- pyarrow, ~90 MB, so a layer or a container image and a build step -- for a
-- format read once and then deleted. See awsnative/backfill/staging.py.
--
-- NO PARTITIONS. Staging is transient. The state machine empties this prefix
-- before the map runs, so the table holds exactly one run's data and the merge
-- reads all of it. A partition scheme here would need a projection config to
-- earn nothing over a prefix that is emptied anyway.
--
-- EVERY COLUMN string, cast in the merge. The archive's decimals arrive as exact
-- text and must reach DECIMAL(38, 18) without passing through a float on the way;
-- declaring them as string is what guarantees Athena does not parse them twice.
--
-- COLUMN ORDER IS THE CONTRACT. There is no header row, so Athena assigns columns
-- by position. This order must match awsnative/backfill/staging.py's
-- KLINE_COLUMNS exactly, and tests/awsnative/test_sql_contracts.py asserts it --
-- because if the two drifted, Athena would put every value in the wrong column
-- and most of the types would still fit.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.archive_staging_klines (
    instrument_id         string,
    open_time_us          string,
    close_time_us         string,
    `open`                string,
    high                  string,
    low                   string,
    `close`               string,
    volume                string,
    quote_volume          string,
    trade_count           string,
    taker_buy_base_volume string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${warehouse}archive_staging_klines/'
