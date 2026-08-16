-- Bronze -> Silver, the keyed upsert.
--
-- WHEN NOT MATCHED THEN INSERT, and deliberately no UPDATE branch, because a
-- trade is an IMMUTABLE FACT. The stream copy and the archive copy of aggTrade
-- 12345 carry the same exchange event_ts_us, the same price and the same size;
-- which lands first is immaterial. That is strictly stronger than SCD Type 1,
-- which would overwrite on a newer sequence and therefore needs an argument
-- that sequences tie and never regress. Insert-if-absent needs no such
-- argument (spec 5.2).
--
-- There is NO lateness cutoff, which is the load-bearing correction carried
-- over from the data-layer spec: a watermark would drop every next-day archive
-- row and leave the reconciliation job permanently red with no bug to find.
--
-- WHY THE row_number() SUBQUERY IS NOT OPTIONAL. MERGE's NOT MATCHED branch
-- protects against a duplicate that is already in the target. It does nothing
-- about two rows with the same (venue, trade_id) inside ONE source batch -- a
-- REST repair replaying a trade the stream already sent, say -- and both would
-- be inserted, putting a duplicate key in Silver and breaking the immutability
-- invariant the whole table rests on. Deduping the source first is what closes
-- that. Ordering by ingest_ts_us keeps the choice deterministic across re-runs
-- so a retry cannot pick a different row.
--
-- The window predicate is the cost dial. lookback_days = 1 means today and
-- yesterday, which covers the UTC midnight boundary; re-reading the overlap
-- every five minutes is free of correctness consequences because the merge is
-- idempotent, and cheap because Bronze's partition projection prunes to those
-- two prefixes. Confirm that in the Athena console: "Data scanned" should be
-- megabytes. If it is gigabytes, projection is not pruning and the whole cost
-- model in spec section 10 is wrong.
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
        CAST(r.price AS DECIMAL(38, 18))  AS price,
        CAST(r."size" AS DECIMAL(38, 18)) AS "size",
        r.side,
        r.sequence,
        r.is_backfill,
        r.source
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.venue, b.trade_id
                ORDER BY b.ingest_ts_us ASC
            ) AS rn
        FROM ${database}.bronze_trades_stream b
        WHERE b.ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
          AND COALESCE(${valid_expr}, false)
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
