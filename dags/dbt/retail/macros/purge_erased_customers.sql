{#
    GDPR right to erasure, applied to the RAW layer too (silver/gold are views or rebuilt from it).
    Run after each load:  dbt run-operation purge_erased_customers
    Note: older Delta versions still contain the data until VACUUM removes them (7 days by default).
#}
{% macro purge_erased_customers() %}
    {% set customers = source('bronze', 'crm_customers') %}
    {% set requests = source('bronze', 'crm_gdpr_requests') %}
    {% set sql %}
        update {{ customers }}
        set first_name = null,
            last_name = null,
            email = null,
            city = null,
            postal_code = null
        where email is not null
          and customer_id in (select customer_id from {{ requests }})
    {% endset %}
    {% set result = run_query(sql) %}
    {{ log("GDPR: PII purged in " ~ customers ~ " for every customer with an erasure request", info=True) }}
{% endmacro %}
