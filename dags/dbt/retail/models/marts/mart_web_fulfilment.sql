-- E-commerce operations: volumes, cancellations and delivery lead times per order day.
select
    to_date(created_at)                                                            as order_date,
    count(*)                                                                       as orders,
    sum(case when current_status = 'CANCELLED' then 1 else 0 end)                  as cancelled_orders,
    sum(case when is_guest_checkout then 1 else 0 end)                             as guest_orders,
    round(avg((unix_timestamp(shipped_at) - unix_timestamp(paid_at)) / 3600), 1)   as avg_hours_to_ship,
    round(avg((unix_timestamp(delivered_at) - unix_timestamp(shipped_at)) / 3600), 1) as avg_hours_in_transit,
    round(avg(case when delivered_at is not null
                   then case when delivered_at <= paid_at + interval 4 days then 1.0 else 0.0 end end), 4)
                                                                                   as delivered_within_4_days_rate,
    sum(late_event_count)                                                          as late_events
from {{ ref('int_ecom__orders') }}
group by to_date(created_at)
