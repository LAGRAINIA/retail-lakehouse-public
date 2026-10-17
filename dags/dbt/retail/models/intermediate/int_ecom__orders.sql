-- One row per web order: current status (latest event by event time, not by arrival time)
-- and the timestamp of each milestone.
with events as (

    select * from {{ ref('stg_ecom__order_events') }}

),

current_state as (

    select *
    from events
    qualify row_number() over (partition by order_id order by updated_at desc, status_rank desc) = 1

),

milestones as (

    select
        order_id,
        min(case when order_status = 'PAID' then updated_at end)        as paid_at,
        min(case when order_status = 'SHIPPED' then updated_at end)     as shipped_at,
        min(case when order_status = 'DELIVERED' then updated_at end)   as delivered_at,
        min(case when order_status = 'CANCELLED' then updated_at end)   as cancelled_at,
        count(*)                                                        as event_count,
        sum(case when is_late_event then 1 else 0 end)                 as late_event_count,
        max(_ingested_at)                                               as _ingested_at
    from events
    group by order_id

)

select
    c.order_id,
    c.customer_id,
    c.customer_id is null                   as is_guest_checkout,
    c.order_status                          as current_status,
    c.created_at,
    m.paid_at,
    m.shipped_at,
    m.delivered_at,
    m.cancelled_at,
    c.country,
    c.currency,
    c.promo_code,
    c.shipping_method,
    c.pickup_store_id,
    c.shipping_cost,
    c.device,
    c.lines,
    m.event_count,
    m.late_event_count,
    m._ingested_at
from current_state as c
inner join milestones as m
    on m.order_id = c.order_id
