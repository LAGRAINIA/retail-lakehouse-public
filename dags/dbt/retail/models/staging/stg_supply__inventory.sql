with source as (

    select * from {{ source('bronze', 'supply_inventory') }}

)

select
    try_cast(snapshot_date as date)             as snapshot_date,
    store_id,
    sku,
    try_cast(quantity_on_hand as int)           as quantity_on_hand,
    try_cast(quantity_reserved as int)          as quantity_reserved,
    try_cast(extracted_at as timestamp)         as extracted_at,
    _ingested_at

from source
qualify row_number() over (partition by snapshot_date, store_id, sku order by _ingested_at desc) = 1
