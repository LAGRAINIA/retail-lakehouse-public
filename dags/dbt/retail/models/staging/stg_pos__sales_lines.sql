-- One row per store sale line. Fixes applied here:
--   * re-sent files and lines exported twice are deduplicated (first copy wins)
--   * a blank store_id is repaired from the file name
--   * POS v2 columns (loyalty_points) are read only when they exist (schema drift)
with source as (

    select * from {{ source('bronze', 'pos_sales') }}

),

typed as (

    select
        transaction_id,
        try_cast(line_number as int)                                           as line_number,
        coalesce(
            nullif(trim(store_id), ''),
            regexp_extract(_source_file, 'pos_sales_(ST[0-9]+)_', 1)
        )                                                                      as store_id,
        nullif(trim(store_id), '') is null                                     as is_store_id_repaired,
        upper(transaction_type)                                                as transaction_type,
        sku,
        try_cast(quantity as int)                                              as quantity,
        try_cast(unit_price as decimal(12, 2))                                 as unit_price,
        coalesce(try_cast(discount_amount as decimal(12, 2)), 0)               as discount_amount,
        upper(currency)                                                        as currency,
        transaction_ts                                                         as transaction_ts_raw,
        lower(payment_method)                                                  as payment_method,
        nullif(customer_id, '')                                                as customer_id,
        cashier_id,
        {{ column_or_null(source('bronze', 'pos_sales'), 'loyalty_points', 'int') }} as loyalty_points,
        to_date(regexp_extract(_source_file, '_([0-9]{8})(_[a-z]+)?[.]csv$', 1), 'yyyyMMdd') as business_date,
        _source_file rlike '_(resend|dup)[.]csv$'                              as is_from_resent_file,
        _source_file,
        _ingested_at

    from source

),

deduplicated as (

    select *
    from typed
    qualify row_number() over (
        partition by transaction_id, line_number
        order by is_from_resent_file, _ingested_at, _source_file
    ) = 1

)

select
    concat(transaction_id, '-', line_number) as sale_line_id,
    *
from deduplicated
