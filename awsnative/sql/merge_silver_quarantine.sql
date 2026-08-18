-- Bronze -> quarantine, the other half of the split.
--
-- This file and merge_silver_trades.sql must partition the Bronze window
-- exactly between them. They do so by construction: both read the same
-- fragments/valid_trade.sql, one wrapped in COALESCE(..., false) and this one
-- in NOT COALESCE(..., false). tests/awsnative/test_render.py asserts the two
-- rendered predicates are literally X and NOT X, so a change to one cannot fail
-- to change the other.
--
-- A MERGE, not the INSERT spec section 5.4 describes. The spec is right about
-- the shape -- two complementary predicates -- but an INSERT is wrong about
-- repetition: the micro-batch re-reads an overlapping window every five
-- minutes, so a plain INSERT would re-add the same bad row 288 times a day and
-- the quarantine rate would measure the schedule rather than the data.
--
-- Keyed on row_key, a hash of the whole raw tuple, because the obvious key does
-- not work here. A row can be quarantined precisely FOR having a NULL venue or
-- trade_id, and NULL = NULL is never true, so those rows would never match and
-- would duplicate forever. See the DDL for the full note.
--
-- quarantine_reason re-derives the individual checks rather than reading them
-- off the predicate, because a boolean cannot say which conjunct failed. It is
-- therefore the one place in this pair where drift is possible; it is
-- diagnostic only, and falls back to 'unknown' rather than lying.
MERGE INTO ${database}.silver_trades_quarantine t
USING (
    SELECT
        to_hex(md5(to_utf8(concat_ws('|',
            COALESCE(r.venue, ''),
            COALESCE(r.trade_id, ''),
            COALESCE(CAST(r.event_ts_us AS VARCHAR), ''),
            COALESCE(r.price, ''),
            COALESCE(r."size", '')
        )))) AS row_key,
        r.venue,
        r.venue_symbol,
        r.instrument_id,
        r.trade_id,
        r.event_ts_us,
        r.ingest_ts_us,
        r.price,
        r."size",
        r.side,
        r.sequence,
        r.is_backfill,
        r.source,
        r.ingest_date,
        COALESCE(NULLIF(concat_ws(',',
            IF(r.venue IS NULL OR r.trade_id IS NULL, 'null_key', NULL),
            IF(COALESCE(try_cast(r.price AS DECIMAL(38, 18)) > 0, false),
               NULL, 'price'),
            IF(COALESCE(try_cast(r."size" AS DECIMAL(38, 18)) > 0, false),
               NULL, 'size'),
            IF(r.event_ts_us IS NULL, 'event_ts_null', NULL),
            IF(COALESCE(r.event_ts_us < CAST(to_unixtime(
                   CASE r.source
                       WHEN 'STREAM' THEN current_timestamp - interval '1' day
                       ELSE current_timestamp - interval '730' day
                   END) * 1000000 AS BIGINT), false),
               'event_ts_too_old', NULL),
            IF(COALESCE(r.event_ts_us > CAST(to_unixtime(
                   current_timestamp + interval '1' minute) * 1000000 AS BIGINT), false),
               'event_ts_too_new', NULL)
        ), ''), 'unknown') AS quarantine_reason,
        current_timestamp AS quarantined_ts
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.venue, b.trade_id
                ORDER BY b.ingest_ts_us ASC
            ) AS rn
        FROM ${database}.bronze_trades_stream b
        WHERE b.ingest_date >= date_format(current_date - interval '${lookback_days}' day, '%Y-%m-%d')
          AND NOT COALESCE(${valid_expr}, false)
    ) r
    WHERE r.rn = 1
) s
ON t.row_key = s.row_key
WHEN NOT MATCHED THEN
    INSERT (row_key, venue, venue_symbol, instrument_id, trade_id, event_ts_us,
            ingest_ts_us, price, "size", side, sequence, is_backfill, source,
            ingest_date, quarantine_reason, quarantined_ts)
    VALUES (s.row_key, s.venue, s.venue_symbol, s.instrument_id, s.trade_id,
            s.event_ts_us, s.ingest_ts_us, s.price, s."size", s.side,
            s.sequence, s.is_backfill, s.source, s.ingest_date,
            s.quarantine_reason, s.quarantined_ts)
