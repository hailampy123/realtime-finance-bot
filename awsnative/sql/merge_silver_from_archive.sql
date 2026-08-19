-- Hot-tier archive -> Silver. The convergence half of spec §6.
--
-- This is the same insert-if-absent merge as merge_silver_trades.sql, reading a
-- different source. Keeping the verb and the key identical is what makes the
-- convergence claim true: "archive rows arrive days late by design and must
-- collapse onto live rows without double-counting volume" (spec §1). The stream
-- copy and the archive copy of aggTrade 12345 carry the same exchange instant,
-- price and size, so whichever lands first wins and the other is a no-op.
--
-- ON (venue, trade_id), and trade_id IS the aggTradeId. Spec §6.1 names
-- aggTradeId as the hot tier's natural key, and it is the same identifier the
-- live Binance connector puts in trade_id -- which is the entire reason the two
-- paths converge rather than double-count.
--
-- NO LATENESS CUTOFF, carried over from §5.2. A watermark here would drop every
-- next-day archive row and leave the reconciliation job permanently red with no
-- bug to find.
--
-- THE row_number() SUBQUERY IS REQUIRED, NOT DEFENSIVE. Trino's MERGE raises an
-- error when two source rows match one target row, so a duplicate inside the
-- source fails the whole statement rather than inserting twice. Staging is
-- emptied per run and the planner never emits two granularities for one period,
-- so duplicates should not arise -- but "should not" is the wrong standard for
-- something that turns a backfill into a hard failure at minute forty.
--
-- ONLY THE HOT TIER REACHES THIS FILE. Deep-tier klines are bars, not trades, and
-- merging them here would put bar rows in a trade table (spec §6.1). The two
-- tiers stage to two different tables so that mistake is structurally impossible
-- rather than a rule to remember.
MERGE INTO ${database}.silver_trades t
USING (
    SELECT
        r.venue,
        r.venue_symbol,
        r.instrument_id,
        r.trade_id,
        CAST(from_unixtime(r.event_ts_us / 1000000.0) AS TIMESTAMP(6))  AS event_ts,
        CAST(from_unixtime(r.ingest_ts_us / 1000000.0) AS TIMESTAMP(6)) AS ingest_ts,
        r.event_ts_us,
        r.ingest_ts_us,
        r.price,
        r."size",
        r.side,
        r.sequence,
        r.is_backfill,
        r.source
    FROM (
        SELECT
            a.venue,
            a.venue_symbol,
            a.instrument_id,
            a.agg_trade_id                                   AS trade_id,
            CAST(a.event_ts_us AS BIGINT)                    AS event_ts_us,
            CAST(to_unixtime(current_timestamp) * 1000000 AS BIGINT) AS ingest_ts_us,
            CAST(a.price AS DECIMAL(38, 18))                 AS price,
            CAST(a.quantity AS DECIMAL(38, 18))              AS "size",
            a.side,
            -- aggTrades carry no sequence number. NULL is the honest value; a
            -- zero would be a number somebody could later average.
            CAST(NULL AS BIGINT)                             AS sequence,
            true                                             AS is_backfill,
            'ARCHIVE'                                        AS source,
            row_number() OVER (
                PARTITION BY a.venue, a.agg_trade_id
                ORDER BY CAST(a.event_ts_us AS BIGINT) ASC
            ) AS rn
        FROM ${database}.archive_staging_trades a
    ) r
    WHERE r.rn = 1
) s
ON  t.venue    = s.venue
AND t.trade_id = s.trade_id
WHEN NOT MATCHED THEN
    INSERT (venue, venue_symbol, instrument_id, trade_id, event_ts, ingest_ts,
            event_ts_us, ingest_ts_us, price, "size", side, sequence,
            is_backfill, source)
    VALUES (s.venue, s.venue_symbol, s.instrument_id, s.trade_id, s.event_ts,
            s.ingest_ts, s.event_ts_us, s.ingest_ts_us, s.price, s."size",
            s.side, s.sequence, s.is_backfill, s.source)
