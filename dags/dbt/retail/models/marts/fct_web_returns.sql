select
    r.return_id,
    r.order_id,
    r.line_id                                                       as sale_line_id,
    r.sku,
    r.quantity,
    r.return_reason,
    r.return_channel,
    to_date(r.requested_at)                                         as return_date,
    r.requested_at,
    r.currency,
    r.refund_amount                                                 as refund_amount_local,
    cast(round(r.refund_amount / fx.rate_per_eur, 2) as decimal(14, 2)) as refund_amount_eur
from {{ ref('stg_ecom__returns') }} as r
left join {{ ref('int_fx_rates_daily') }} as fx
    on fx.currency = r.currency
    and fx.rate_date = to_date(r.requested_at)
