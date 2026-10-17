{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sale_line_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['sale_date', 'store_id'],
    contract={'enforced': true}
) }}

-- Grain: one sale line (store or web). Amounts in local currency and in EUR.
-- Incremental on ingestion time: a late file or a late web event is picked up by the next run,
-- whatever its business date.
with lines as (

    select *
    from {{ ref('int_sales_lines') }}
    where not has_quality_issue
    {% if is_incremental() %}
      and _ingested_at > (
          select max(_ingested_at) - interval {{ var('incremental_lookback_hours') }} hours from {{ this }}
      )
    {% endif %}

)

select
    cast(l.sale_line_id as string)                                          as sale_line_id,
    cast(l.channel as string)                                               as channel,
    cast(l.order_id as string)                                              as order_id,
    cast(l.store_id as string)                                              as store_id,
    cast(l.customer_id as string)                                           as customer_id,
    cast(p.product_version_key as string)                                   as product_version_key,
    cast(l.sku as string)                                                   as sku,
    cast(l.sale_date as date)                                               as sale_date,
    cast(l.sold_at_utc as timestamp)                                        as sold_at_utc,
    cast(l.is_return as boolean)                                            as is_return,
    cast(l.quantity as int)                                                 as quantity,
    cast(l.currency as string)                                              as currency,
    cast(l.unit_price as decimal(12, 2))                                    as unit_price_local,
    cast(l.discount_amount as decimal(12, 2))                               as discount_local,
    cast(l.net_amount_local as decimal(14, 2))                              as net_amount_local,
    cast(round(l.net_amount_local / fx.rate_per_eur, 2) as decimal(14, 2))  as net_amount_eur,
    cast(round(l.net_amount_local / fx.rate_per_eur - l.quantity * p.unit_cost_eur, 2) as decimal(14, 2))
                                                                            as gross_margin_eur,
    cast(l.is_store_id_repaired as boolean)                                 as is_store_id_repaired,
    cast(l._ingested_at as timestamp)                                       as _ingested_at
from lines as l
left join {{ ref('int_fx_rates_daily') }} as fx
    on fx.currency = l.currency
    and fx.rate_date = l.sale_date
left join {{ ref('int_products_scd2') }} as p
    on p.sku = l.sku
    and l.sale_date >= p.valid_from
    and (p.valid_to is null or l.sale_date < p.valid_to)
