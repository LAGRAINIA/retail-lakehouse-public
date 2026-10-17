-- CRM change feed: several versions per customer. PII is purged at source on erasure requests
-- (macro purge_erased_customers), so it may be NULL here.
with source as (

    select * from {{ source('bronze', 'crm_customers') }}

)

select
    customer_id,
    upper(op)                                       as change_type,
    first_name,
    last_name,
    lower(email)                                    as email,
    try_cast(birth_year as int)                     as birth_year,
    upper(country)                                  as country,
    city,
    postal_code,
    lower(loyalty_tier)                             as loyalty_tier,
    try_cast(marketing_opt_in as boolean)           as marketing_opt_in,
    try_cast(signup_date as date)                   as signup_date,
    try_cast(updated_at as timestamp)               as updated_at,
    _source_file,
    _ingested_at

from source
qualify row_number() over (partition by customer_id, updated_at order by _ingested_at desc) = 1
