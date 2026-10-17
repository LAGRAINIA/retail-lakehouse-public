-- GDPR: a customer who asked for erasure must have no personal data left in silver.
select customer_id
from {{ ref('int_customers_current') }}
where is_erased
  and (first_name is not null or last_name is not null or email is not null or postal_code is not null)
