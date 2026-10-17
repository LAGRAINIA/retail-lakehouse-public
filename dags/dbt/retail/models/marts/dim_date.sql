with days as (

    {{ dbt_utils.date_spine(datepart="day",
                            start_date="cast('2026-01-01' as date)",
                            end_date="cast('2028-01-01' as date)") }}

)

select
    cast(date_day as date)                                  as date_day,
    cast(date_format(date_day, 'yyyyMMdd') as int)          as date_key,
    year(date_day)                                          as year,
    quarter(date_day)                                       as quarter,
    month(date_day)                                         as month,
    date_format(date_day, 'MMMM')                           as month_name,
    weekofyear(date_day)                                    as iso_week,
    dayofweek(date_day)                                     as day_of_week,      -- 1 = Sunday
    date_format(date_day, 'EEEE')                           as day_name,
    dayofweek(date_day) in (1, 7)                           as is_weekend
from days
