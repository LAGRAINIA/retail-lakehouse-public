{#
    Schema drift: a column that only exists in recent files (e.g. POS v2 'loyalty_points').
    Returns the typed column when it exists in the relation, a typed NULL otherwise, so the
    model keeps compiling before AND after the source changes.
#}
{% macro column_or_null(relation, column_name, data_type) -%}
    {%- set existing = [] -%}
    {%- if execute -%}
        {%- set existing = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- endif -%}
    {%- if column_name | lower in existing -%}
        try_cast({{ adapter.quote(column_name) }} as {{ data_type }})
    {%- else -%}
        cast(null as {{ data_type }})
    {%- endif -%}
{%- endmacro %}
