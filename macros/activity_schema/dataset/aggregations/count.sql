{% macro aggfunc_count(column) %}
    {{ return(adapter.dispatch("aggfunc_count", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_count(column) %}
count({{ column.column_sql }})
{%- endmacro -%}
