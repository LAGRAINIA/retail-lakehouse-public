-- Web order lines (nested array exploded), with the promo discount from the reference seed.
with orders as (

    select * from {{ ref('int_ecom__orders') }}

),

exploded as (

    select
        o.order_id,
        o.customer_id,
        o.created_at,
        o.paid_at,
        o.current_status,
        o.currency,
        o.promo_code,
        o.shipping_method,
        o.pickup_store_id,
        o._ingested_at,
        line.line_id,
        line.sku,
        try_cast(line.quantity as int)                  as quantity,
        try_cast(line.unit_price as decimal(12, 2))     as unit_price
    from orders as o
    lateral view explode(o.lines) as line

)

select
    e.*,
    coalesce(pc.discount_pct, 0)                        as discount_pct
from exploded as e
left join {{ ref('promo_codes') }} as pc
    on pc.promo_code = e.promo_code
    and to_date(e.created_at) >= pc.valid_from
    and (pc.valid_to is null or to_date(e.created_at) <= pc.valid_to)
