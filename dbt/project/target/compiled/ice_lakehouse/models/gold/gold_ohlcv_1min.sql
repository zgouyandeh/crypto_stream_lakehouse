-- 1-minute OHLCV + volume-weighted average price per product. Rebuilt in
-- full from silver each run (config: materialized = table, inherited from
-- dbt_project.yml).

with trades as (

    select * from silver.stg_trades

),

bucketed as (

    select
        *,
        date_trunc('minute', time) as bucket,
        row_number() over (partition by product_id, date_trunc('minute', time) order by time asc)  as rn_asc,
        row_number() over (partition by product_id, date_trunc('minute', time) order by time desc) as rn_desc
    from trades

)

select
    product_id,
    bucket,
    max(case when rn_asc  = 1 then price end) as open,
    max(price)                                 as high,
    min(price)                                 as low,
    max(case when rn_desc = 1 then price end) as close,
    sum(size)                                  as volume,
    sum(price * size) / nullif(sum(size), 0)   as vwap,
    count(*)                                   as trade_count
from bucketed
group by product_id, bucket