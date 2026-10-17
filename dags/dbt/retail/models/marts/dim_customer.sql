-- No direct PII in gold: analysts get segments, not names or emails.
select
    customer_id,
    country,
    city,
    case
        when birth_year is null then 'unknown'
        when year(current_date()) - birth_year < 25 then '18-24'
        when year(current_date()) - birth_year < 35 then '25-34'
        when year(current_date()) - birth_year < 50 then '35-49'
        when year(current_date()) - birth_year < 65 then '50-64'
        else '65+'
    end                         as age_band,
    loyalty_tier,
    marketing_opt_in,
    signup_date,
    is_erased
from {{ ref('int_customers_current') }}
