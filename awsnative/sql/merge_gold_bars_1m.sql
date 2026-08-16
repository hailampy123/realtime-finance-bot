-- Silver -> Gold, rebuilding only the dirty partitions.
--
-- A note on quoting, because Athena is two dialects wearing one coat: DDL is
-- Hive-flavoured and quotes identifiers with `backticks`; DML is Trino and
-- quotes them with "double quotes". Same column, different quote character,
-- depending on the file you are in. "size", "open" and "close" are quoted here
-- purely so nothing depends on remembering which words a future engine version
-- decides to reserve.
--
-- Idempotent by construction: one MERGE keyed on (instrument_id,
-- window_end_ts), one Iceberg commit, so re-running a failed execution is safe
-- and the state machine can retry it blindly.
--
-- HOW THIS STAYS CHEAP. Only partitions named by the dirty CTE are touched.
-- (A placeholder is never written inside a comment in these files: rendering
-- would splice a multi-line fragment into a single-line -- comment and silently
-- swallow the statement after it.) The join
-- is on Silver's two partition columns -- instrument_id and the day() transform
-- of event_ts -- so Trino's dynamic filtering prunes Silver to just those
-- partitions rather than scanning the table. This is the single assumption in
-- the file worth verifying rather than trusting: check "Data scanned" in the
-- Athena console against the size of one symbol-day. If it matches the whole
-- table, dynamic filtering is not engaging and the fallback is to interpolate
-- an explicit IN-list of partitions.
--
-- OPEN AND CLOSE ARE TIE-BROKEN ON PURPOSE. event_ts is millisecond-precision,
-- and at crypto trade rates several trades routinely share a millisecond. Plain
-- min_by(price, event_ts) would pick among them arbitrarily, so re-running the
-- same merge could write a DIFFERENT open for the same bar -- churn that looks
-- like market data changing retroactively. Ordering by the microsecond
-- timestamp and then by trade_id makes the choice total and stable.
--
-- The UPDATE branch is unconditional, which is correct while this file is the
-- only Gold writer. Stage N4 adds a second one: klines merged with source_tier
-- = ARCHIVE_KLINE, which is LOWER fidelity than DERIVED_FROM_TRADES and must
-- not overwrite it. That guard belongs in N4's merge, not here -- adding it now
-- would be a branch no test could reach.
MERGE INTO ${database}.gold_bars_1m g
USING (
    WITH dirty AS (
        ${dirty_cte}
    ),
    bars AS (
        SELECT
            s.instrument_id,
            date_trunc('minute', s.event_ts) + interval '1' minute AS window_end_ts,
            min_by(s.price, ROW(s.event_ts_us, s.trade_id))        AS bar_open,
            max(s.price)                                           AS bar_high,
            min(s.price)                                           AS bar_low,
            max_by(s.price, ROW(s.event_ts_us, s.trade_id))        AS bar_close,
            sum(s."size")                                          AS volume,
            sum(
                CAST(s.price AS DECIMAL(29, 9))
                * CAST(s."size" AS DECIMAL(29, 9))
            ) AS notional,
            sum(IF(s.side = 'BUY',  s."size", DECIMAL '0'))        AS buy_vol,
            sum(IF(s.side = 'SELL', s."size", DECIMAL '0'))        AS sell_vol,
            count(*)                                               AS trade_count,
            CAST(count(DISTINCT s.venue) AS INTEGER)               AS venue_coverage
        FROM ${database}.silver_trades s
        JOIN dirty d
          ON  s.instrument_id = d.instrument_id
          AND CAST(s.event_ts AS DATE) = d.dt
        GROUP BY s.instrument_id, date_trunc('minute', s.event_ts)
    )
    SELECT
        instrument_id,
        window_end_ts,
        bar_open,
        bar_high,
        bar_low,
        bar_close,
        volume,
        notional,
        buy_vol,
        sell_vol,
        -- Guarded because a zero or negative price would make ln() fail the
        -- whole query, and price > 0 is enforced upstream by the validity
        -- contract -- so this branch should be unreachable, and is here so that
        -- a hole in that contract degrades one bar instead of the pipeline.
        CASE WHEN bar_open > 0 AND bar_close > 0
             THEN power(ln(CAST(bar_close AS DOUBLE) / CAST(bar_open AS DOUBLE)), 2)
             ELSE 0.0
        END AS sq_log_return,
        trade_count,
        venue_coverage,
        'DERIVED_FROM_TRADES' AS source_tier,
        current_timestamp     AS updated_ts
    FROM bars
) s
ON  g.instrument_id = s.instrument_id
AND g.window_end_ts = s.window_end_ts
WHEN MATCHED THEN UPDATE SET
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
