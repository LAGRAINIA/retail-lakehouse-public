-- One row per order status change (CDC feed). Some events arrive days late: the event time
-- (updated_at), not the arrival time, decides the order of events.
with source as (

    select * from {{ source('bronze', 'ecom_orders') }}

),

typed as (

    select
        order_id,
        customer_id,
        upper(order_status)                                     as order_status,
        try_cast(created_at as timestamp)                       as created_at,
        try_cast(updated_at as timestamp)                       as updated_at,
        upper(country)                                          as country,
        upper(currency)                                         as currency,
        promo_code,
        shipping.method                                         as shipping_method,
        shipping.pickup_store_id                                as pickup_store_id,
        try_cast(shipping.cost as decimal(10, 2))               as shipping_cost,
        lines,
        {{ column_or_null(source('bronze', 'ecom_orders'), 'device', 'string') }} as device,
        case upper(order_status)
            when 'CREATED' then 1 when 'PAID' then 2 when 'SHIPPED' then 3
            when 'DELIVERED' then 4 when 'CANCELLED' then 5
        end                                                     as status_rank,
        to_date(regexp_extract(_source_file, '_([0-9]{8})[.]json$', 1), 'yyyyMMdd') as landing_date,
        _source_file,
        _ingested_at

    from source

)

select
    *,
    landing_date > to_date(updated_at) as is_late_event
from typed
-- the same event can be delivered twice by the source: keep one
qualify row_number() over (partition by order_id, order_status, updated_at order by _ingested_at) = 1
