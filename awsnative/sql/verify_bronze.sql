-- Stage N1 acceptance queries. Run each and compare against the expectations in
-- the plan; they check three different failure modes, so run all three.

-- 1. Rows are arriving, and from both venues.
--    A missing venue means one connector failed silently -- the runner logs on
--    reconnect, not on "never connected".
SELECT venue,
       count(*)                AS rows,
       count(DISTINCT trade_id) AS distinct_trades,
       min(from_unixtime(event_ts_us / 1000000)) AS first_event,
       max(from_unixtime(event_ts_us / 1000000)) AS last_event
FROM bronze_trades_stream
GROUP BY venue
ORDER BY venue;

-- 2. No column is systematically null.
--    Firehose's OpenX deserializer silently writes NULL for a JSON key that does
--    not match a Glue column, so a rename shows up here and nowhere else.
SELECT count(*)                                         AS rows,
       count_if(venue IS NULL)                          AS null_venue,
       count_if(instrument_id IS NULL)                  AS null_instrument,
       count_if(trade_id IS NULL)                       AS null_trade_id,
       count_if(event_ts_us IS NULL)                    AS null_event_ts,
       count_if(price IS NULL)                          AS null_price,
       count_if("size" IS NULL)                         AS null_size,
       count_if(side IS NULL)                           AS null_side,
       count_if(source IS NULL)                         AS null_source,
       count_if(schema_version IS NULL)                 AS null_schema_version
FROM bronze_trades_stream;

-- 3. The values are sane, not merely present.
--    Zero or negative prices, or timestamps outside a plausible epoch range,
--    mean the encoder or a parser is wrong even though the pipe works.
SELECT count_if(CAST(price AS DECIMAL(38, 18)) <= 0)   AS nonpositive_price,
       count_if(CAST("size" AS DECIMAL(38, 18)) <= 0)  AS nonpositive_size,
       count_if(side NOT IN ('BUY', 'SELL', 'UNKNOWN')) AS bad_side,
       count_if(event_ts_us < 1500000000000000)         AS ts_too_old,
       count_if(event_ts_us > 1900000000000000)         AS ts_too_new,
       count_if(schema_version <> 1)                    AS wrong_schema_version
FROM bronze_trades_stream;
