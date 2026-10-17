with source as (

    select * from {{ source('bronze', 'ecom_returns') }}

)

select
    return_id,
    order_id,
    line_id,
    sku,
    try_cast(quantity as int)                     as quantity,
    lower(reason)                                 as return_reason,
    try_cast(requested_at as timestamp)           as requested_at,
    lower(return_channel)                         as return_channel,
    try_cast(refund_amount as decimal(12, 2))     as refund_amount,
    upper(currency)                               as currency,
    _source_file,
    _ingested_at

from source
qualify row_number() over (partition by return_id order by _ingested_at) = 1
