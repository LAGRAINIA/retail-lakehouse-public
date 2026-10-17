select
    store_id,
    store_name,
    city,
    country,
    timezone,
    currency,
    opening_date,
    store_status,
    surface_m2
from {{ ref('int_stores_current') }}
