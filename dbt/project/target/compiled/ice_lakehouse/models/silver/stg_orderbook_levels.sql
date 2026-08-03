

-- Flattens two different message shapes into one row-per-price-level table:
--   snapshot:  {"bids": [[price, size], ...], "asks": [[price, size], ...]}
--   l2update:  {"changes": [[side, price, size], ...]}
--
-- Design choice: a size of 0 means "this price level was removed", but a
-- plain MERGE has no natural DELETE branch in dbt-spark's default merge
-- strategy, so removed levels are kept as rows with size = 0 rather than
-- deleted. Downstream consumers (see gold_top_of_book) filter `size > 0` to
-- get the currently-active book. A true delete-on-zero would need a custom
-- merge SQL statement, which is a reasonable next step if you want it.

with snap as (

    select
        product_id,
        ingest_time,
        from_json(payload, 'struct<bids:array<array<string>>,asks:array<array<string>>>') as parsed
    from bronze.orderbook_events
    where type = 'snapshot'

    
    and ingest_time > (select coalesce(max(event_time), timestamp('1970-01-01')) from silver.stg_orderbook_levels)
    

),

snap_bids as (
    select
        product_id,
        'buy' as side,
        level[0] as price_str,
        level[1] as size_str,
        ingest_time as event_time,
        cast(null as bigint) as sequence
    from snap
    lateral view explode(parsed.bids) t as level
),

snap_asks as (
    select
        product_id,
        'sell' as side,
        level[0] as price_str,
        level[1] as size_str,
        ingest_time as event_time,
        cast(null as bigint) as sequence
    from snap
    lateral view explode(parsed.asks) t as level
),

upd as (
    select
        product_id,
        sequence,
        -- to_timestamp returns null on a parse failure rather than
        -- raising, so a malformed/missing `time` falls back to ingest_time
        coalesce(to_timestamp(time), ingest_time) as event_time,
        from_json(payload, 'struct<changes:array<array<string>>>').changes as changes
    from bronze.orderbook_events
    where type = 'l2update'

    
    and ingest_time > (select coalesce(max(event_time), timestamp('1970-01-01')) from silver.stg_orderbook_levels)
    

),

upd_levels as (
    select
        product_id,
        change[0] as side,
        change[1] as price_str,
        change[2] as size_str,
        event_time,
        sequence
    from upd
    lateral view explode(changes) t as change
),

combined as (
    select product_id, side, cast(price_str as double) as price, cast(size_str as double) as size, event_time, sequence from snap_bids
    union all
    select product_id, side, cast(price_str as double) as price, cast(size_str as double) as size, event_time, sequence from snap_asks
    union all
    select product_id, side, cast(price_str as double) as price, cast(size_str as double) as size, event_time, sequence from upd_levels
),

deduped as (
    select
        *,
        row_number() over (
            partition by product_id, side, price
            order by event_time desc, sequence desc nulls last
        ) as rn
    from combined
)

select product_id, side, price, size, event_time, sequence
from deduped
where rn = 1