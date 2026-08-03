

-- Cleans and deduplicates bronze.trades. trade_id should already be unique
-- from Coinbase, but a reconnect-triggered resubscribe could in principle
-- redeliver a trade near the boundary, so dedup here rather than assume.

with source as (

    select *
    from bronze.trades
    where trade_id   is not null
      and product_id is not null
      and time       is not null

    
    and ingest_time > (select coalesce(max(ingest_time), timestamp('1970-01-01')) from silver.stg_trades)
    

),

deduped as (

    select
        *,
        row_number() over (partition by trade_id order by ingest_time desc) as rn
    from source

)

select
    trade_id,
    sequence,
    product_id,
    price,
    size,
    side,
    time,
    ingest_time
from deduped
where rn = 1