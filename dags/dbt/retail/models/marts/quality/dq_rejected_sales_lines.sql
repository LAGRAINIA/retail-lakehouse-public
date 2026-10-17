-- Quarantine: sale lines excluded from fct_sales_lines, with the reason. Lets the business see and
-- fix bad source data instead of silently losing it.
select
    sale_line_id,
    channel,
    order_id,
    store_id,
    sku,
    quantity,
    unit_price,
    sale_date,
    concat_ws(', ',
        case when is_unknown_product then 'unknown_product' end,
        case when is_zero_quantity then 'zero_quantity' end,
        case when is_negative_price then 'negative_price' end,
        case when is_missing_store then 'missing_store' end,
        case when is_missing_timestamp then 'missing_timestamp' end
    )                                       as rejection_reasons,
    _ingested_at
from {{ ref('int_sales_lines') }}
where has_quality_issue
