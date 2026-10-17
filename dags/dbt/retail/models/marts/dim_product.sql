-- One row per product VERSION (SCD2): facts point to the version valid on the sale date,
-- so a margin computed last month does not change when the cost changes today.
select
    product_version_key,
    sku,
    product_name,
    sport,
    category,
    brand,
    unit_price_eur,
    unit_cost_eur,
    product_status,
    launch_date,
    valid_from,
    valid_to,
    is_current
from {{ ref('int_products_scd2') }}
