{% macro aggfunc_sum(column) %}
    {{ return(adapter.dispatch("aggfunc_sum", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_sum(column) %}
sum({{ column.column_sql }})
{%- endmacro -%}
