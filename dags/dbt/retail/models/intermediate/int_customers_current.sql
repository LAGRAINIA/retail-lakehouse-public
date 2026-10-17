-- Current state of every customer, from the CRM change feed. Customers with an erasure request
-- are kept (sales still reference them) but every personal attribute is removed.
with latest as (

    select *
    from {{ ref('stg_crm__customer_changes') }}
    qualify row_number() over (partition by customer_id order by updated_at desc, _ingested_at desc) = 1

),

erasures as (

    select customer_id, min(requested_at) as erasure_requested_at
    from {{ ref('stg_crm__gdpr_requests') }}
    where request_type = 'erasure'
    group by customer_id

)

select
    l.customer_id,
    e.customer_id is not null                                       as is_erased,
    e.erasure_requested_at,
    case when e.customer_id is null then l.first_name end           as first_name,
    case when e.customer_id is null then l.last_name end            as last_name,
    case when e.customer_id is null then l.email end                as email,
    case when e.customer_id is null then l.city end                 as city,
    case when e.customer_id is null then l.postal_code end          as postal_code,
    case when e.customer_id is null then l.birth_year end           as birth_year,
    l.country,
    l.loyalty_tier,
    case when e.customer_id is null then l.marketing_opt_in else false end as marketing_opt_in,
    l.signup_date,
    l.updated_at
from latest as l
left join erasures as e
    on e.customer_id = l.customer_id
