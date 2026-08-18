-- Bronze -> Silver for perpetual context. Insert-if-absent on the 5-minute grid.
--
-- WHY INSERT-ONLY. A reading is an immutable observation: what the venue said at
-- 05:25 never changes. Re-reading an overlapping Bronze window every five minutes
-- is therefore free of correctness consequences, exactly as it is for
-- merge_silver_trades.sql, and a retry converges rather than double-counting.
--
-- knowledge_ts_us IS THE POLL INSTANT, not the grid point. The reading describes
-- 05:25 but only became retrievable when the poll ran, which is later. Read paths
-- filter on knowledge_ts so a point-in-time query cannot see a reading before it
-- existed. Taking the EARLIEST poll for a grid point is what makes that boundary
-- as tight as the data allows.
--
-- try_cast, NOT cast, and the reason is worth stating because the trade-off is
-- real. There is no quarantine table for this data (it is informational, spec
-- §1), so a single unparseable value under a plain CAST would fail the whole
-- merge and stop enrichment for every instrument. try_cast turns that into a NULL
-- for one column of one row. The cost is that a systematic format change would be
-- silent, which is why verify_enrichment.sql counts rows where Bronze held a
-- non-empty string and Silver holds NULL. A silent drop that is measured is a
-- different thing from one that is not.
--
-- NULLIF(x, '') FIRST. The collector writes an empty string when an endpoint did
-- not answer, and empty is not zero: a long/short ratio of zero would say every
-- account is short. NULLIF makes "absent" reach Silver as NULL.
MERGE INTO ${database}.silver_perp_context t
USING (
    SELECT
        r.instrument_id,
        r.venue,
        r.venue_symbol,
        CAST(from_unixtime(r.snapshot_ts_us / 1000000.0) AS TIMESTAMP(6)) AS snapshot_ts,
        r.snapshot_ts_us,
        r.knowledge_ts_us,
        r.mark_price,
        r.index_price,
        r.funding_rate,
        r.interest_rate,
        r.next_funding_ts,
        r.open_interest,
        r.toptrader_long_accounts,
        r.toptrader_short_accounts,
        r.toptrader_ratio_accounts,
        r.toptrader_long_positions,
        r.toptrader_short_positions,
        r.toptrader_ratio_positions,
        r.global_long_accounts,
        r.global_short_accounts,
        r.global_ratio_accounts,
        r.taker_buy_vol,
        r.taker_sell_vol,
        r.taker_buy_sell_ratio,
        r.source_tier
    FROM (
        SELECT
            b.instrument_id,
            b.venue,
            b.venue_symbol,
            CAST(b.snapshot_ts_us AS BIGINT)                              AS snapshot_ts_us,
            CAST(b.poll_ts_us AS BIGINT)                                  AS knowledge_ts_us,
            try_cast(NULLIF(b.mark_price, '') AS DECIMAL(38, 18))         AS mark_price,
            try_cast(NULLIF(b.index_price, '') AS DECIMAL(38, 18))        AS index_price,
            try_cast(NULLIF(b.last_funding_rate, '') AS DECIMAL(38, 18))  AS funding_rate,
            try_cast(NULLIF(b.interest_rate, '') AS DECIMAL(38, 18))      AS interest_rate,
            CAST(from_unixtime(
                CAST(NULLIF(b.next_funding_time_us, '') AS BIGINT) / 1000000.0
            ) AS TIMESTAMP(6))                                            AS next_funding_ts,
            try_cast(NULLIF(b.open_interest, '') AS DECIMAL(38, 18))      AS open_interest,
            try_cast(NULLIF(b.toptrader_accounts_long, '') AS DECIMAL(38, 18))   AS toptrader_long_accounts,
            try_cast(NULLIF(b.toptrader_accounts_short, '') AS DECIMAL(38, 18))  AS toptrader_short_accounts,
            try_cast(NULLIF(b.toptrader_accounts_ratio, '') AS DECIMAL(38, 18))  AS toptrader_ratio_accounts,
            try_cast(NULLIF(b.toptrader_positions_long, '') AS DECIMAL(38, 18))  AS toptrader_long_positions,
            try_cast(NULLIF(b.toptrader_positions_short, '') AS DECIMAL(38, 18)) AS toptrader_short_positions,
            try_cast(NULLIF(b.toptrader_positions_ratio, '') AS DECIMAL(38, 18)) AS toptrader_ratio_positions,
            try_cast(NULLIF(b.global_accounts_long, '') AS DECIMAL(38, 18))      AS global_long_accounts,
            try_cast(NULLIF(b.global_accounts_short, '') AS DECIMAL(38, 18))     AS global_short_accounts,
            try_cast(NULLIF(b.global_accounts_ratio, '') AS DECIMAL(38, 18))     AS global_ratio_accounts,
            try_cast(NULLIF(b.taker_volume_long, '') AS DECIMAL(38, 18))         AS taker_buy_vol,
            try_cast(NULLIF(b.taker_volume_short, '') AS DECIMAL(38, 18))        AS taker_sell_vol,
            try_cast(NULLIF(b.taker_volume_ratio, '') AS DECIMAL(38, 18))        AS taker_buy_sell_ratio,
            'LIVE_POLL'                                                   AS source_tier,
            row_number() OVER (
                PARTITION BY b.instrument_id, b.snapshot_ts_us
                ORDER BY CAST(b.poll_ts_us AS BIGINT) ASC
            ) AS rn
        FROM ${database}.bronze_perp_context b
        WHERE b.ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
          AND b.instrument_id IS NOT NULL
          AND b.snapshot_ts_us IS NOT NULL
    ) r
    WHERE r.rn = 1
) s
ON  t.instrument_id  = s.instrument_id
AND t.snapshot_ts_us = s.snapshot_ts_us
WHEN NOT MATCHED THEN
    INSERT (instrument_id, venue, venue_symbol, snapshot_ts, snapshot_ts_us,
            knowledge_ts_us, mark_price, index_price, funding_rate, interest_rate,
            next_funding_ts, open_interest, toptrader_long_accounts,
            toptrader_short_accounts, toptrader_ratio_accounts,
            toptrader_long_positions, toptrader_short_positions,
            toptrader_ratio_positions, global_long_accounts, global_short_accounts,
            global_ratio_accounts, taker_buy_vol, taker_sell_vol,
            taker_buy_sell_ratio, source_tier)
    VALUES (s.instrument_id, s.venue, s.venue_symbol, s.snapshot_ts, s.snapshot_ts_us,
            s.knowledge_ts_us, s.mark_price, s.index_price, s.funding_rate, s.interest_rate,
            s.next_funding_ts, s.open_interest, s.toptrader_long_accounts,
            s.toptrader_short_accounts, s.toptrader_ratio_accounts,
            s.toptrader_long_positions, s.toptrader_short_positions,
            s.toptrader_ratio_positions, s.global_long_accounts, s.global_short_accounts,
            s.global_ratio_accounts, s.taker_buy_vol, s.taker_sell_vol,
            s.taker_buy_sell_ratio, s.source_tier)
