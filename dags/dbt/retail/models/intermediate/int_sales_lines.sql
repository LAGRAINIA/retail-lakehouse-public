-- Every sale line of both channels, in one shape, with data quality flags.
-- Lines with a quality issue are routed to quality.dq_rejected_sales_lines instead of the fact.
with store_lines as (

    select
        sale_line_id,
        'store'                                         as channel,
        transaction_id                                  as order_id,
        store_id,
        customer_id,
        sku,
        transaction_type = 'RETURN'                     as is_return,
        quantity,
        unit_price,
        discount_amount,
        currency,
        sold_at_utc,
        business_date                                   as sale_date,
        is_store_id_repaired,
        _ingested_at
    from {{ ref('int_pos__sales_lines') }}

),

web_lines as (

    select
        line_id                                         as sale_line_id,
        'web'                                           as channel,
        order_id,
        pickup_store_id                                 as store_id,     -- click & collect only
        customer_id,
        sku,
        false                                           as is_return,
        quantity,
        unit_price,
        cast(round(unit_price * quantity * discount_pct, 2) as decimal(12, 2)) as discount_amount,
        currency,
        paid_at                                         as sold_at_utc,
        to_date(paid_at)                                as sale_date,
        false                                           as is_store_id_repaired,
        _ingested_at
    from {{ ref('int_ecom__order_lines') }}
    where paid_at is not null                           -- a sale is recognised at payment

),

unioned as (

    select * from store_lines
    union all
    select * from web_lines

),

known_products as (

    select distinct sku from {{ ref('int_products_scd2') }}

)

select
    u.*,
    cast(u.unit_price * u.quantity - u.discount_amount as decimal(14, 2))   as net_amount_local,
    k.sku is null                                                          as is_unknown_product,
    coalesce(u.quantity, 0) = 0                                            as is_zero_quantity,
    u.unit_price < 0                                                       as is_negative_price,
    u.channel = 'store' and u.store_id is null                             as is_missing_store,
    u.sold_at_utc is null                                                  as is_missing_timestamp,
    (k.sku is null
        or coalesce(u.quantity, 0) = 0
        or u.unit_price < 0
        or (u.channel = 'store' and u.store_id is null)
        or u.sold_at_utc is null)                                          as has_quality_issue
from unioned as u
left join known_products as k
    on k.sku = u.sku
