{{ config(severity='warn') }}
-- Completeness: every open store must have sales for every day up to the day before the latest
-- sale date (a file one day late is normal, two days is an incident).
with bounds as (
    select min(sale_date) as first_day, max(sale_date) - interval 1 day as last_complete_day
    from {{ ref('fct_sales_lines') }}
),
expected as (
    select s.store_id, d.date_day
    from {{ ref('dim_store') }} as s
    cross join {{ ref('dim_date') }} as d
    cross join bounds as b
    where d.date_day between greatest(b.first_day, s.opening_date) and b.last_complete_day
      and s.store_status = 'open'
),
actual as (
    select distinct store_id, sale_date
    from {{ ref('fct_sales_lines') }}
    where channel = 'store'
)
select e.store_id, e.date_day as missing_day
from expected as e
left join actual as a
    on a.store_id = e.store_id
    and a.sale_date = e.date_day
where a.store_id is null
