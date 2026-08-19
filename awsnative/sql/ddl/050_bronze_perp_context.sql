-- Bronze: raw Binance perpetual context, one wide row per instrument per poll.
--
-- Plain text (gzipped JSON Lines), not Iceberg, following D1: append-only data
-- needs no MERGE, no time travel and no snapshot expiry.
--
-- WRITTEN BY A LAMBDA, NOT FIREHOSE, which narrows §6.2 of the enrichment design.
-- That section routed the poller through a second Firehose delivery stream to
-- buffer away a small-file problem. Measured, the problem is not there: 288 polls
-- a day at ~4 KB each is 1.2 MB a day, and the merge reads only today's
-- partition. Dropping Firehose removes a delivery stream, its IAM role, and the
-- Glue table its record-format conversion would have required.
--
-- FOUR SOURCE TIMESTAMPS ARE KEPT, and that is spec §7.1's requirement rather
-- than redundancy. The ratio endpoints answer on their own 5-minute grid, which
-- need not equal the poll instant. `snapshot_ts_us` is the grid point the poll
-- floors to; the four `*_ts_us` columns are what each endpoint actually said.
-- Collapsing them would destroy the only evidence that would show the grids had
-- drifted apart, which is assumption X1.
--
-- EVERY COLUMN string. Decimals reach DECIMAL(38, 18) in the merge without
-- passing through a float on the way -- the same discipline Bronze already
-- applies to trade price and size.
--
-- Partition projection rather than a crawler, matching bronze_trades_stream: the
-- catalog can never disagree with S3 because it computes locations rather than
-- listing them.
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.bronze_perp_context (
    instrument_id             string,
    venue                     string,
    venue_symbol              string,
    poll_ts_us                string,
    snapshot_ts_us            string,
    mark_price                string,
    index_price               string,
    estimated_settle_price    string,
    last_funding_rate         string,
    interest_rate             string,
    next_funding_time_us      string,
    premium_index_ts_us       string,
    open_interest             string,
    open_interest_ts_us       string,
    toptrader_accounts_ratio  string,
    toptrader_accounts_long   string,
    toptrader_accounts_short  string,
    toptrader_accounts_ts_us  string,
    toptrader_positions_ratio string,
    toptrader_positions_long  string,
    toptrader_positions_short string,
    toptrader_positions_ts_us string,
    global_accounts_ratio     string,
    global_accounts_long      string,
    global_accounts_short     string,
    global_accounts_ts_us     string,
    taker_volume_ratio        string,
    taker_volume_long         string,
    taker_volume_short        string,
    taker_volume_ts_us        string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${warehouse}bronze_perp_context/'
TBLPROPERTIES (
    'projection.enabled'                   = 'true',
    'projection.ingest_date.type'          = 'date',
    'projection.ingest_date.format'        = 'yyyy-MM-dd',
    'projection.ingest_date.range'         = '${projection_start_date},NOW',
    'projection.ingest_date.interval'      = '1',
    'projection.ingest_date.interval.unit' = 'DAYS',
    'storage.location.template'            = '${warehouse}bronze_perp_context/ingest_date=$${ingest_date}/'
)
