-- Deep-tier archive -> Gold, directly. The second Gold writer (spec §6.1).
--
-- Deep-tier klines feed gold_bars_1m and NOT silver_trades, because they are bars
-- rather than trades. Two years of 1-minute bars is what makes a backtest
-- possible after a weekly wipe; two years of trades is not something this archive
-- publishes at a size worth downloading.
--
-- THE FIDELITY GUARD IS THE POINT OF THIS FILE. merge_gold_bars_1m.sql:33-37
-- states the requirement and defers it here: "klines merged with source_tier =
-- ARCHIVE_KLINE ... is LOWER fidelity than DERIVED_FROM_TRADES and must not
-- overwrite it." A kline bar is aggregated by the exchange at bar granularity; a
-- DERIVED_FROM_TRADES bar is computed from individual trades and carries a real
-- venue_coverage and a trade-level open and close. Inside the hot window both
-- exist for the same minute, and that overlap IS the reconciliation (§6.4) -- so
-- letting the archive overwrite the stream would delete the very rows the
-- reconciliation compares, and the check would then pass by comparing klines to
-- themselves. Spec §6.4: "a green check that cannot fail is worse than no check,
-- because it is trusted."
--
-- WINDOW ALIGNMENT, the one detail that must match the stream path exactly.
-- merge_gold_bars_1m.sql computes `date_trunc('minute', event_ts) + interval '1'
-- minute`, so the bar covering 00:00:00 has window_end_ts 00:01:00. A kline's
-- openTime is that minute's start and its closeTime is 00:00:59.999999. Deriving
-- window_end_ts from closeTime would therefore be one microsecond short of the
-- stream's value, the MERGE key would never match, and Gold would hold two rows
-- per minute -- silently, and only inside the hot window. It is derived from
-- openTime, and date_trunc is applied anyway so alignment cannot depend on the
-- publisher having emitted an exactly aligned openTime.
--
-- ADDITIVE COMPONENTS, taken from the archive rather than recomputed:
--   notional  = quoteAssetVolume, which is exactly SUM(price * size) for the bar
--   buy_vol   = takerBuyBaseAssetVolume
--   sell_vol  = volume - takerBuyBaseAssetVolume, the only arithmetic here
-- Storing components rather than ratios is §5.3's rule, and it applies to this
-- writer identically.
--
-- venue_coverage = 1 because only Binance publishes an archive of this shape. It
-- is not a placeholder: a reader comparing venue_coverage across tiers is
-- reading a true statement about how many venues contributed.
--
-- The row_number() dedupe is required for the same reason as in
-- merge_silver_from_archive.sql: Trino's MERGE fails when two source rows match
-- one target row.
MERGE INTO ${database}.gold_bars_1m g
USING (
    SELECT
        r.instrument_id,
        r.window_end_ts,
        r.bar_open,
        r.bar_high,
        r.bar_low,
        r.bar_close,
        r.volume,
        r.notional,
        r.buy_vol,
        r.sell_vol,
        -- Guarded exactly as the stream path guards it: a zero or negative price
        -- would fail ln() for the whole query, so one bad bar degrades to a zero
        -- instead of killing the statement.
        CASE WHEN r.bar_open > 0 AND r.bar_close > 0
             THEN power(ln(CAST(r.bar_close AS DOUBLE) / CAST(r.bar_open AS DOUBLE)), 2)
             ELSE 0.0
        END AS sq_log_return,
        r.trade_count,
        r.venue_coverage,
        r.source_tier,
        r.updated_ts
    FROM (
        SELECT
            k.instrument_id,
            date_trunc(
                'minute',
                CAST(from_unixtime(CAST(k.open_time_us AS BIGINT) / 1000000.0) AS TIMESTAMP(6))
            ) + interval '1' minute                          AS window_end_ts,
            CAST(k."open" AS DECIMAL(38, 18))                AS bar_open,
            CAST(k.high AS DECIMAL(38, 18))                  AS bar_high,
            CAST(k.low AS DECIMAL(38, 18))                   AS bar_low,
            CAST(k."close" AS DECIMAL(38, 18))               AS bar_close,
            CAST(k.volume AS DECIMAL(38, 18))                AS volume,
            CAST(k.quote_volume AS DECIMAL(38, 18))          AS notional,
            CAST(k.taker_buy_base_volume AS DECIMAL(38, 18)) AS buy_vol,
            CAST(k.volume AS DECIMAL(38, 18))
              - CAST(k.taker_buy_base_volume AS DECIMAL(38, 18)) AS sell_vol,
            CAST(k.trade_count AS BIGINT)                    AS trade_count,
            CAST(1 AS INTEGER)                               AS venue_coverage,
            'ARCHIVE_KLINE'                                  AS source_tier,
            current_timestamp                                AS updated_ts,
            row_number() OVER (
                PARTITION BY k.instrument_id, k.open_time_us
                ORDER BY CAST(k.open_time_us AS BIGINT) ASC
            ) AS rn
        FROM ${database}.archive_staging_klines k
    ) r
    WHERE r.rn = 1
) s
ON  g.instrument_id = s.instrument_id
AND g.window_end_ts = s.window_end_ts
-- THE GUARD. An existing DERIVED_FROM_TRADES bar is higher fidelity and is left
-- alone; an existing ARCHIVE_KLINE bar is refreshed, which keeps a re-run
-- idempotent rather than a no-op.
WHEN MATCHED AND g.source_tier <> 'DERIVED_FROM_TRADES' THEN UPDATE SET
    "open"         = s.bar_open,
    high           = s.bar_high,
    low            = s.bar_low,
    "close"        = s.bar_close,
    volume         = s.volume,
    notional       = s.notional,
    buy_vol        = s.buy_vol,
    sell_vol       = s.sell_vol,
    sq_log_return  = s.sq_log_return,
    trade_count    = s.trade_count,
    venue_coverage = s.venue_coverage,
    source_tier    = s.source_tier,
    updated_ts     = s.updated_ts
WHEN NOT MATCHED THEN
    INSERT (instrument_id, window_end_ts, "open", high, low, "close", volume,
            notional, buy_vol, sell_vol, sq_log_return, trade_count,
            venue_coverage, source_tier, updated_ts)
    VALUES (s.instrument_id, s.window_end_ts, s.bar_open, s.bar_high, s.bar_low,
            s.bar_close, s.volume, s.notional, s.buy_vol, s.sell_vol,
            s.sq_log_return, s.trade_count, s.venue_coverage, s.source_tier,
            s.updated_ts)
