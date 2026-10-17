-- Latest known state of every store (from the most recent ERP extract it appears in).
select *
from {{ ref('stg_erp__stores') }}
qualify row_number() over (partition by store_id order by extract_date desc) = 1
