-- SCD2 integrity: exactly one current version per product.
select sku, count(*) as current_versions
from {{ ref('int_products_scd2') }}
where is_current
group by sku
having count(*) <> 1
