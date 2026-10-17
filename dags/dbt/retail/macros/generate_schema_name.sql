{#
    prod target: schemas are named after the layer (silver, gold, quality, snapshots).
    any other target: prefixed with the target schema (dev_silver, dev_gold...), so a developer
    never overwrites production tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
