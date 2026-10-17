with source as (

    select * from {{ source('bronze', 'erp_stores') }}

)

select
    store_id,
    store_name,
    city,
    upper(country)                                                      as country,
    timezone,
    upper(currency)                                                     as currency,
    try_cast(opening_date as date)                                      as opening_date,
    lower(status)                                                       as store_status,
    try_cast(surface_m2 as int)                                         as surface_m2,
    try_cast(updated_at as timestamp)                                   as updated_at,
    to_date(regexp_extract(_source_file, '_([0-9]{8})[.]csv$', 1), 'yyyyMMdd') as extract_date,
    _ingested_at

from source
qualify row_number() over (partition by store_id, extract_date order by _ingested_at desc) = 1
