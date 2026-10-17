{#
    Deployed environments (dev, uat, prod) each have their own catalog, so schemas are simply named
    after the layer: silver, gold, quality, snapshots.
    The 'local' target (a developer's sandbox) prefixes them with the target schema (local_silver...),
    so local work never collides with a deployed environment.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name in ['dev', 'uat', 'prod'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
