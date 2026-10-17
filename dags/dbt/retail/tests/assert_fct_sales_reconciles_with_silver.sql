-- Incremental drift: the fact must contain exactly the valid lines of silver. A gap means an
-- incremental run missed data (wrong filter, late data) and a full refresh or backfill is needed.
with silver as (
    select count(*) as n from {{ ref('int_sales_lines') }} where not has_quality_issue
),
gold as (
    select count(*) as n from {{ ref('fct_sales_lines') }}
)
select silver.n as silver_lines, gold.n as gold_lines
from silver cross join gold
where silver.n <> gold.n
