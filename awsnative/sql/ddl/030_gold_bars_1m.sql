-- Gold: 1-minute bars, stored as additive components.
--
-- The rule this table exists to enforce (spec 5.3): store numerators and
-- denominators, never precomputed ratios. A `vwap` column would be correct at
-- 1 minute and wrong at every other grain, because averaging an average is not
-- an average -- and it fails QUIETLY, which is the part that matters. Storing
-- notional and volume instead means SUM(notional)/SUM(volume) is right at 1
-- minute, 5 minutes, an hour, or a day.
--
--   VWAP            = SUM(notional) / SUM(volume)
--   Realized vol    = SQRT(SUM(sq_log_return))
--   Flow imbalance  = (SUM(buy_vol) - SUM(sell_vol)) / SUM(volume)
--
-- awsnative/bars.py is the executable statement of that contract, and
-- tests/awsnative/test_sql_contracts.py asserts this DDL declares every
-- component it names. Adding a measure to one and not the other fails offline.
--
-- THE ONE NON-ADDITIVE EXCEPTION, stated rather than hidden: `open` and `close`
-- are first/last by event time and cannot be re-derived when rolling 1-minute
-- bars up to 5. Read paths therefore expose OHLC at 1-minute grain ONLY; any
-- coarser rollup returns VWAP, volume and imbalance but no OHLC. Saying so is
-- the alternative to silently returning a wrong `open`.
--
-- source_tier is the fidelity marker (spec 6.4). DERIVED_FROM_TRADES bars come
-- from silver_trades; ARCHIVE_KLINE bars arrive in stage N4 straight from
-- Binance klines, at bar granularity rather than trade granularity. Without
-- this column a two-year backtest silently mixes the two and the model appears
-- to improve over time when all that improved is the input data.
--
-- sq_log_return is the bar-internal squared log return, ln(close/open)^2. It
-- does NOT capture the return from one bar's close to the next bar's open, so
-- SQRT(SUM(...)) understates realized vol by the overnight/inter-bar component.
-- That is the estimator spec 5.3 specifies; the gap is real and worth knowing
-- before quoting the number.
CREATE TABLE IF NOT EXISTS ${database}.gold_bars_1m (
    instrument_id  string,
    window_end_ts  timestamp,
    `open`         decimal(38, 18),
    high           decimal(38, 18),
    low            decimal(38, 18),
    `close`        decimal(38, 18),
    volume         decimal(38, 18),
    notional       decimal(38, 18),
    buy_vol        decimal(38, 18),
    sell_vol       decimal(38, 18),
    sq_log_return  double,
    trade_count    bigint,
    venue_coverage int,
    source_tier    string,
    updated_ts     timestamp
)
PARTITIONED BY (instrument_id, day(window_end_ts))
LOCATION '${warehouse}gold_bars_1m/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
