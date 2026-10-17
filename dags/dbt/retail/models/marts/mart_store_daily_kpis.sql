-- One row per store and day: the dashboard of store managers.
with sales as (

    select
        sale_date,
        store_id,
        count(distinct case when not is_return then order_id end)          as transactions,
        sum(case when not is_return then quantity else 0 end)              as units_sold,
        sum(case when is_return then -quantity else 0 end)                 as units_returned,
        sum(net_amount_eur)                                                as net_revenue_eur,
        sum(gross_margin_eur)                                              as gross_margin_eur,
        count(distinct customer_id)                                        as identified_customers
    from {{ ref('fct_sales_lines') }}
    where channel = 'store'
    group by sale_date, store_id

),

stock as (

    select
        snapshot_date,
        store_id,
        avg(case when is_stockout then 1.0 else 0.0 end)                   as stockout_rate
    from {{ ref('fct_inventory_daily') }}
    group by snapshot_date, store_id

)

select
    s.sale_date,
    s.store_id,
    st.store_name,
    st.country,
    s.transactions,
    s.units_sold,
    s.units_returned,
    s.net_revenue_eur,
    s.gross_margin_eur,
    round(s.net_revenue_eur / nullif(s.transactions, 0), 2)                as average_basket_eur,
    round(s.gross_margin_eur / nullif(s.net_revenue_eur, 0), 4)            as gross_margin_rate,
    s.identified_customers,
    round(k.stockout_rate, 4)                                              as stockout_rate
from sales as s
left join {{ ref('dim_store') }} as st
    on st.store_id = s.store_id
left join stock as k
    on k.store_id = s.store_id
    and k.snapshot_date = s.sale_date
