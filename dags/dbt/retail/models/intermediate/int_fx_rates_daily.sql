-- One rate per calendar day and currency: rates are only published on business days, so weekends
-- and holidays reuse the last published rate. EUR is added with a rate of 1.
with bounds as (

    select min(rate_date) as first_day, current_date() as last_day
    from {{ ref('stg_finance__fx_rates') }}

),

calendar as (

    select explode(sequence(first_day, last_day, interval 1 day)) as rate_date
    from bounds

),

currencies as (

    select distinct currency from {{ ref('stg_finance__fx_rates') }}

),

filled as (

    select
        c.rate_date,
        cur.currency,
        last_value(r.rate_per_eur, true) over (
            partition by cur.currency
            order by c.rate_date
            rows between unbounded preceding and current row
        )                                           as rate_per_eur,
        r.rate_per_eur is null                      as is_carried_forward
    from calendar as c
    cross join currencies as cur
    left join {{ ref('stg_finance__fx_rates') }} as r
        on r.rate_date = c.rate_date
        and r.currency = cur.currency

)

select * from filled

union all

select rate_date, 'EUR' as currency, cast(1 as decimal(12, 6)) as rate_per_eur, false as is_carried_forward
from calendar
