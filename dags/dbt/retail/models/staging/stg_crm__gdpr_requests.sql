with source as (

    select * from {{ source('bronze', 'crm_gdpr_requests') }}

)

select
    request_id,
    customer_id,
    lower(request_type)                     as request_type,
    try_cast(requested_at as timestamp)     as requested_at,
    _ingested_at

from source
qualify row_number() over (partition by request_id order by _ingested_at) = 1
