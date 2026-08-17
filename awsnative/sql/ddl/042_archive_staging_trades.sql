-- Hot-tier staging: aggTrades as they came out of the archive, side already resolved.
--
-- Same three choices as archive_staging_klines: external gzipped CSV, no
-- partitions, every column a string. Read that file's header for the reasons.
--
-- WHY THIS IS A SEPARATE TABLE FROM archive_staging_klines (narrows §3.1's single
-- `archive_staging`). klines are bars and aggTrades are trades. The two have
-- different columns and different meanings, and one Glue table cannot describe
-- both. Spec §6.1 already forbids conflating them for the stronger reason: "Only
-- hot-tier aggTrades merge into Silver. Conflating the two would put bar rows in
-- a trade table." Two tables make that structurally impossible rather than a rule
-- somebody has to remember.
--
-- `side` ARRIVES ALREADY DERIVED. The archive carries `isBuyerMaker`; this column
-- carries BUY or SELL. The inversion happens once, in
-- awsnative/backfill/parsers.py `_side_from_buyer_maker`, and a golden-file test
-- pins it. Doing it in SQL instead would put the trap in two places, because the
-- stream path has no equivalent field to invert.
--
-- COLUMN ORDER IS THE CONTRACT. Must match staging.py's TRADE_COLUMNS exactly;
-- tests/awsnative/test_sql_contracts.py asserts it.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.archive_staging_trades (
    instrument_id  string,
    venue          string,
    venue_symbol   string,
    agg_trade_id   string,
    price          string,
    quantity       string,
    first_trade_id string,
    last_trade_id  string,
    event_ts_us    string,
    side           string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${warehouse}archive_staging_trades/'
