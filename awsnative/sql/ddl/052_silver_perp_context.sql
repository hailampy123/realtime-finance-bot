-- Silver: perpetual context at the 5-minute grid. Positioning, in one table.
--
-- ONE TABLE, NOT TWO, which narrows §7.2 of the enrichment design. That section
-- split funding (8-hour grain) from positioning (5-minute grain) on the belief
-- that a funding rate is a settled fact. It is not: `lastFundingRate` from
-- /fapi/v1/premiumIndex is the running quote for the UPCOMING settlement and
-- moves continuously until it settles. Keyed on an 8-hour settlement instant with
-- an insert-only merge, the first estimate of the interval would be frozen and
-- every later revision to it silently discarded. Keyed on the 5-minute grid the
-- quote is an immutable observation -- what the venue said at 05:25 never
-- changes -- so insert-only is correct and the whole quote series is preserved.
--
-- The settled 8-hour series is a different thing and comes from a different
-- source: the monthly `fundingRate` archive files carry calc_time,
-- funding_interval_hours and the applied rate. When that tier is loaded it earns
-- its own table rather than sharing this one, because a settled rate and a quote
-- are not the same measurement.
--
-- COMPONENTS AND RATIOS BOTH (spec §4.2). Three of the four endpoints return the
-- numerator and denominator alongside the ratio. Storing the components is what
-- makes a rollup over these rows valid at all: an average of ratios is not the
-- ratio of the aggregates, which is the same rule that forbids a stored vwap in
-- gold_bars_1m. A reader aggregating across time must sum the components and
-- divide, never average the ratio column.
--
-- A NULL COMPONENT MEANS "the source did not carry it", NEVER "the value was
-- zero". A long/short ratio of zero says every account is short. The archive tier
-- will fill the ratio and leave the components NULL, which is why `source_tier`
-- exists here as it does on gold_bars_1m.
--
-- open_interest_value IS NOT STORED. It is open_interest * mark_price, both of
-- which are here, and §5.3's rule is that a value derivable from stored columns
-- is derived by the reader rather than frozen at one grain.
--
-- knowledge_ts_us IS THE POINT-IN-TIME BOUNDARY. It is the poll instant: the
-- earliest moment this reading was retrievable. Read paths filter on it, never on
-- snapshot_ts, because the grid point a reading describes is earlier than the
-- moment it became observable.
CREATE TABLE IF NOT EXISTS ${database}.silver_perp_context (
    instrument_id             string,
    venue                     string,
    venue_symbol              string,
    snapshot_ts               timestamp,
    snapshot_ts_us            bigint,
    knowledge_ts_us           bigint,
    mark_price                decimal(38, 18),
    index_price               decimal(38, 18),
    funding_rate              decimal(38, 18),
    interest_rate             decimal(38, 18),
    next_funding_ts           timestamp,
    open_interest             decimal(38, 18),
    toptrader_long_accounts   decimal(38, 18),
    toptrader_short_accounts  decimal(38, 18),
    toptrader_ratio_accounts  decimal(38, 18),
    toptrader_long_positions  decimal(38, 18),
    toptrader_short_positions decimal(38, 18),
    toptrader_ratio_positions decimal(38, 18),
    global_long_accounts      decimal(38, 18),
    global_short_accounts     decimal(38, 18),
    global_ratio_accounts     decimal(38, 18),
    taker_buy_vol             decimal(38, 18),
    taker_sell_vol            decimal(38, 18),
    taker_buy_sell_ratio      decimal(38, 18),
    source_tier               string
)
PARTITIONED BY (instrument_id, day(snapshot_ts))
LOCATION '${warehouse}silver_perp_context/'
TBLPROPERTIES (
    'table_type'        = 'ICEBERG',
    'format'            = 'parquet',
    'write_compression' = 'snappy'
)
