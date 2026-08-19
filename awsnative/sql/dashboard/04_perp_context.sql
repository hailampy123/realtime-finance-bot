-- The showcase: what the enrichment lets a reader see that the tape cannot.
--
-- Three measures for one instrument on one time axis, returned long-form so the
-- page can draw three stacked panels rather than one chart with three y-scales.
-- That is not a rendering preference: price, a funding rate near zero, and an
-- open interest in the tens of thousands share no scale, and forcing them onto
-- two axes would let the author pick the correlation the reader sees.
--
-- What the three together answer: price rising while open interest rises is new
-- money entering; price rising while open interest falls is shorts covering, and
-- the move has spent itself. Funding says who is paying to hold the position.
-- gold_bars_1m alone cannot distinguish any of these.
--
-- knowledge_ts, not snapshot_ts, is the honest filter for a point-in-time read.
-- This is a monitoring view of the recent past, so it reads snapshot_ts for a
-- natural x-axis and is deliberately NOT the query a backtest should copy.
SELECT
    to_unixtime(p.snapshot_ts)      AS ts,
    date_format(p.snapshot_ts, '%Y-%m-%d %H:%i') AS label,
    CAST(p.mark_price AS DOUBLE)    AS mark_price,
    CAST(p.funding_rate AS DOUBLE)  AS funding_rate,
    CAST(p.open_interest AS DOUBLE) AS open_interest
FROM ${database}.silver_perp_context p
WHERE p.instrument_id = '${instrument_id}'
  AND p.snapshot_ts >= current_timestamp - interval '${lookback_days}' day
ORDER BY p.snapshot_ts
