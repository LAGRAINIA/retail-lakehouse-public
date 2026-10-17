{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='inventory_key',
    liquid_clustered_by=['snapshot_date', 'store_id']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'store_id', 'sku']) }}   as inventory_key,
    snapshot_date,
    store_id,
    sku,
    quantity_on_hand,
    quantity_reserved,
    greatest(quantity_on_hand - quantity_reserved, 0)                             as quantity_available,
    quantity_on_hand = 0                                                          as is_stockout,
    quantity_on_hand < 0                                                          as is_negative_stock,
    _ingested_at
from {{ ref('stg_supply__inventory') }}
{% if is_incremental() %}
where _ingested_at > (
    select max(_ingested_at) - interval {{ var('incremental_lookback_hours') }} hours from {{ this }}
)
{% endif %}
