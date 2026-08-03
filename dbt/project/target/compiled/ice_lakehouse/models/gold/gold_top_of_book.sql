-- Best bid, best ask, and spread per product, reconstructed from the
-- currently-active (size > 0) order book state in silver. Rebuilt in full
-- each run.

with active_levels as (

    select *
    from silver.stg_orderbook_levels
    where size > 0

),

best as (

    select
        product_id,
        max(case when side = 'buy'  then price end) as best_bid,
        min(case when side = 'sell' then price end) as best_ask
    from active_levels
    group by product_id

)

select
    product_id,
    best_bid,
    best_ask,
    best_ask - best_bid as spread
from best