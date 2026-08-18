-- The validity contract for a Bronze row, as one boolean expression over the
-- alias `b`. No trailing semicolon: this is an expression, not a statement.
--
-- It lives in its own file, rather than as text in two merge files, because
-- Silver and quarantine must partition the input EXACTLY. A row that this
-- expression neither accepts nor rejects is a silently dropped trade, which
-- spec section 5.4 forbids outright. Two copies of a predicate are two things
-- that can drift; one copy cannot.
--
-- Both merge files wrap it identically:
--     merge_silver_trades.sql      AND      COALESCE(<this>, false)
--     merge_silver_quarantine.sql  AND NOT  COALESCE(<this>, false)
--
-- The COALESCE is load-bearing, not defensive noise. Firehose's OpenX
-- deserializer writes NULL for any JSON key that does not match a Glue column,
-- and NULL propagates through both a predicate AND its negation -- so without
-- it, a malformed row is rejected by `p` and also rejected by `NOT p`, and
-- lands in neither table. That is exactly the silent drop 5.4 rules out.
--
-- The recency bound is source-dependent on purpose (spec 5.4). A uniform
-- [now - 1d, now + 1m] would quarantine every archive backfill row, because
-- archives are published next-day -- the same class of contradiction the
-- data-layer spec caught in the Databricks watermark.
    try_cast(b.price AS DECIMAL(38, 18)) > 0
AND try_cast(b."size" AS DECIMAL(38, 18)) > 0
AND b.event_ts_us >= CAST(
        to_unixtime(
            CASE b.source
                WHEN 'STREAM' THEN current_timestamp - interval '1' day
                ELSE current_timestamp - interval '730' day
            END
        ) * 1000000 AS BIGINT)
AND b.event_ts_us <= CAST(
        to_unixtime(current_timestamp + interval '1' minute) * 1000000 AS BIGINT)
