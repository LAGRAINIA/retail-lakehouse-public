-- Store lines with a proper UTC timestamp. POS v1 sends local wall-clock time without offset,
-- POS v2 sends ISO 8601 with an offset: both are converted to UTC with the store time zone.
select
    p.*,
    case
        when p.transaction_ts_raw rlike '[+-][0-9]{2}:[0-9]{2}$'
            then try_cast(p.transaction_ts_raw as timestamp)
        else to_utc_timestamp(try_to_timestamp(p.transaction_ts_raw, 'yyyy-MM-dd HH:mm:ss'), s.timezone)
    end                                                         as sold_at_utc,
    s.timezone                                                  as store_timezone
from {{ ref('stg_pos__sales_lines') }} as p
left join {{ ref('int_stores_current') }} as s
    on s.store_id = p.store_id
