-- Product history (SCD type 2) rebuilt from the daily full extracts: a new version starts every
-- time a tracked attribute changes (price, status, name, category). Rebuilding from extracts,
-- instead of a dbt snapshot, keeps the full history even when a month of files is backfilled at once.
with extracts as (

    select
        *,
        concat_ws('|', product_name, category, cast(unit_price_eur as string), product_status) as tracked_state
    from {{ ref('stg_erp__products') }}

),

changes as (

    select
        *,
        lag(tracked_state) over (partition by sku order by extract_date) as previous_state
    from extracts

),

versions as (

    select *
    from changes
    where previous_state is null or previous_state <> tracked_state

)

select
    {{ dbt_utils.generate_surrogate_key(['sku', 'extract_date']) }}            as product_version_key,
    sku,
    product_name,
    sport,
    category,
    brand,
    unit_price_eur,
    unit_cost_eur,
    product_status,
    launch_date,
    extract_date                                                               as valid_from,
    lead(extract_date) over (partition by sku order by extract_date)           as valid_to,
    lead(extract_date) over (partition by sku order by extract_date) is null   as is_current
from versions
