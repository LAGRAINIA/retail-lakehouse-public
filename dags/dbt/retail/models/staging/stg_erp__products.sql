-- Every daily ERP extract (not only the latest): the product history (SCD2) is rebuilt from them.
with source as (

    select * from {{ source('bronze', 'erp_products') }}

)

select
    sku,
    product_name,
    lower(sport)                                                        as sport,
    lower(category)                                                     as category,
    brand,
    try_cast(unit_price_eur as decimal(12, 2))                          as unit_price_eur,
    try_cast(unit_cost_eur as decimal(12, 2))                           as unit_cost_eur,
    lower(status)                                                       as product_status,
    try_cast(launch_date as date)                                       as launch_date,
    try_cast(last_modified as date)                                     as last_modified,
    to_date(regexp_extract(_source_file, '_([0-9]{8})[.]csv$', 1), 'yyyyMMdd') as extract_date,
    _source_file,
    _ingested_at

from source
qualify row_number() over (partition by sku, extract_date order by _ingested_at desc) = 1
