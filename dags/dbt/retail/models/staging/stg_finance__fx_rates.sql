-- One row per (date, currency). Rates are "1 EUR = x currency"; published on business days only.
with source as (

    select * from {{ source('bronze', 'finance_fx_rates') }}

),

unpivoted as (

    select
        try_cast(rate_date as date)                 as rate_date,
        stack(3,
            'GBP', try_cast(rates.GBP as decimal(12, 6)),
            'CHF', try_cast(rates.CHF as decimal(12, 6)),
            'USD', try_cast(rates.USD as decimal(12, 6))
        )                                           as (currency, rate_per_eur),
        _ingested_at
    from source

)

select *
from unpivoted
qualify row_number() over (partition by rate_date, currency order by _ingested_at desc) = 1
