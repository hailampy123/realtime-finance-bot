-- Stage N2/N3 acceptance queries.
--
-- Run all of them. They check different failure modes and several are designed
-- so that a broken pipeline still returns rows -- a query that cannot fail is
-- worse than no query, because it gets trusted.
--
-- Query 2 is the immutability tripwire and query 3 is the no-silent-drop check;
-- those two are the stage gate. The rest are coverage and cost.
-- Query 6 is the N3 gate: it is the only one that proves the additive
-- decomposition actually decomposes.


-- ---------------------------------------------------------------- N2 ------
-- 1. Silver is being written, from both venues, and is recent.
--    A venue missing here but present in Bronze means the merge dropped it;
--    both venues missing means the state machine never ran.
SELECT venue,
       count(*)                 AS rows,
       count(DISTINCT trade_id) AS distinct_trades,
       min(event_ts)            AS first_event,
       max(event_ts)            AS last_event,
       max(ingest_ts)           AS last_ingest
FROM silver_trades
GROUP BY venue
ORDER BY venue;


-- 2. THE IMMUTABILITY TRIPWIRE. Must return zero rows.
--    silver_trades has no UPDATE branch because a trade is an immutable fact:
--    the stream copy and the archive copy of the same trade are supposed to be
--    byte-identical in the fields that matter. If they are not, that assumption
--    is broken, and it has to be discovered here rather than by a backtest that
--    silently double-counts volume.
--
--    A non-zero result is a finding, not a warning to suppress.
SELECT venue,
       trade_id,
       count(*)                       AS row_count,
       count(DISTINCT event_ts_us)    AS distinct_event_ts,
       count(DISTINCT price)          AS distinct_price,
       count(DISTINCT "size")         AS distinct_size
FROM silver_trades
GROUP BY venue, trade_id
HAVING count(*) > 1
    OR count(DISTINCT event_ts_us) > 1
    OR count(DISTINCT price) > 1
    OR count(DISTINCT "size") > 1
LIMIT 100;


-- 3. NOTHING WAS SILENTLY DROPPED. `unaccounted` must be 0.
--    Every distinct (venue, trade_id) that reached Bronze must be in exactly
--    one of silver_trades or silver_trades_quarantine. This is the query that
--    actually tests the COALESCE in fragments/valid_trade.sql -- remove it and
--    NULL-bearing rows vanish from both tables and show up here.
--
--    Caveat, and it is a real one: rows that landed in Bronze after the last
--    micro-batch execution have not been merged yet and count as unaccounted.
--    Re-run this a minute after a completed execution before believing a
--    non-zero number.
WITH bronze_keys AS (
    SELECT DISTINCT venue, trade_id
    FROM bronze_trades_stream
    WHERE ingest_date >= date_format(current_date - interval '1' day, '%Y-%m-%d')
),
silver_keys AS (
    SELECT DISTINCT venue, trade_id FROM silver_trades
),
quarantine_keys AS (
    SELECT DISTINCT venue, trade_id FROM silver_trades_quarantine
)
SELECT
    (SELECT count(*) FROM bronze_keys)     AS bronze_distinct_keys,
    (SELECT count(*) FROM silver_keys)     AS silver_distinct_keys,
    (SELECT count(*) FROM quarantine_keys) AS quarantine_distinct_keys,
    (SELECT count(*)
     FROM bronze_keys b
     WHERE NOT EXISTS (SELECT 1 FROM silver_keys s
                       WHERE s.venue = b.venue AND s.trade_id = b.trade_id)
       AND NOT EXISTS (SELECT 1 FROM quarantine_keys q
                       WHERE q.venue = b.venue AND q.trade_id = b.trade_id)
    ) AS unaccounted;


-- 4. Quarantine rate and why.
--    Expect this to be empty or near-empty on live stream data. A rate above a
--    fraction of a percent means the encoder, the Glue schema and the validity
--    contract disagree about something -- read the reasons, they name the
--    conjunct that failed.
--
--    A rate that grows while Bronze is idle means a row with a NULL natural key
--    is being re-inserted; see the row_key note in the quarantine DDL.
SELECT quarantine_reason,
       count(*)          AS rows,
       min(quarantined_ts) AS first_seen,
       max(quarantined_ts) AS last_seen
FROM silver_trades_quarantine
GROUP BY quarantine_reason
ORDER BY rows DESC;


-- ---------------------------------------------------------------- N3 ------
-- 5. Gold is being rebuilt, and how far behind it is.
--    bars_behind tells you whether the 5-minute cadence is keeping up. A few
--    minutes is expected: Firehose buffers 120s, then the schedule fires.
SELECT instrument_id,
       count(*)              AS bars,
       min(window_end_ts)    AS first_bar,
       max(window_end_ts)    AS last_bar,
       max(updated_ts)       AS last_rebuild,
       date_diff('minute', max(window_end_ts), current_timestamp) AS minutes_behind,
       sum(trade_count)      AS trades_covered,
       max(venue_coverage)   AS max_venues_in_a_bar
FROM gold_bars_1m
GROUP BY instrument_id
ORDER BY instrument_id;


-- 6. THE N3 GATE: the additive decomposition actually decomposes.
--    Roll the 1-minute bars up to 5 minutes from the stored components, and
--    compute the same 5-minute measures directly from silver_trades. They must
--    agree to floating-point noise.
--
--    vwap_abs_diff and imbalance_abs_diff should be < 1e-9. If they are not,
--    Gold is storing something it should not -- most likely a ratio.
--
--    Note there is no OHLC column in this comparison, and that is the point:
--    open and close are first/last by event time and are NOT recoverable from
--    1-minute bars at 5-minute grain (spec 5.3). Read paths expose OHLC at
--    1-minute grain only.
WITH gold_5m AS (
    SELECT
        instrument_id,
        date_add('minute',
                 -(minute(window_end_ts - interval '1' minute) % 5),
                 date_trunc('minute', window_end_ts - interval '1' minute)) AS bucket,
        sum(notional)                            AS notional,
        sum(volume)                              AS volume,
        sum(buy_vol)                             AS buy_vol,
        sum(sell_vol)                            AS sell_vol,
        sum(trade_count)                         AS trade_count
    FROM gold_bars_1m
    WHERE source_tier = 'DERIVED_FROM_TRADES'
    GROUP BY 1, 2
),
silver_5m AS (
    SELECT
        instrument_id,
        date_add('minute',
                 -(minute(event_ts) % 5),
                 date_trunc('minute', event_ts)) AS bucket,
        sum(
            CAST(price AS DECIMAL(29, 9))
            * CAST("size" AS DECIMAL(29, 9))
        ) AS notional,
        sum("size")                              AS volume,
        sum(IF(side = 'BUY',  "size", DECIMAL '0')) AS buy_vol,
        sum(IF(side = 'SELL', "size", DECIMAL '0')) AS sell_vol,
        count(*)                                 AS trade_count
    FROM silver_trades
    GROUP BY 1, 2
)
SELECT
    g.instrument_id,
    g.bucket,
    g.trade_count AS gold_trades,
    s.trade_count AS silver_trades,
    abs(CAST(g.notional / g.volume AS DOUBLE)
        - CAST(s.notional / s.volume AS DOUBLE))              AS vwap_abs_diff,
    abs(CAST((g.buy_vol - g.sell_vol) / g.volume AS DOUBLE)
        - CAST((s.buy_vol - s.sell_vol) / s.volume AS DOUBLE)) AS imbalance_abs_diff
FROM gold_5m g
JOIN silver_5m s
  ON  g.instrument_id = s.instrument_id
  AND g.bucket = s.bucket
WHERE g.volume > 0 AND s.volume > 0
ORDER BY vwap_abs_diff DESC
LIMIT 20;


-- 7. Why Gold does not store a vwap column, demonstrated rather than asserted.
--    naive_vwap averages the per-minute VWAPs; correct_vwap divides the summed
--    components. They differ whenever volume is uneven across the minutes,
--    which is always. This query exists so the reason for spec 5.3 is visible
--    in your own data rather than taken on faith.
WITH per_minute AS (
    SELECT instrument_id,
           date_add('minute',
                    -(minute(window_end_ts - interval '1' minute) % 5),
                    date_trunc('minute', window_end_ts - interval '1' minute)) AS bucket,
           notional / volume AS minute_vwap,
           notional,
           volume
    FROM gold_bars_1m
    WHERE volume > 0 AND source_tier = 'DERIVED_FROM_TRADES'
)
SELECT instrument_id,
       bucket,
       CAST(avg(minute_vwap) AS DOUBLE)              AS naive_vwap,
       CAST(sum(notional) / sum(volume) AS DOUBLE)   AS correct_vwap,
       CAST(avg(minute_vwap) - sum(notional) / sum(volume) AS DOUBLE) AS error
FROM per_minute
GROUP BY instrument_id, bucket
HAVING count(*) > 1
ORDER BY abs(avg(minute_vwap) - sum(notional) / sum(volume)) DESC
LIMIT 10;
