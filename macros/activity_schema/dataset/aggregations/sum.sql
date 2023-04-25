{% macro aggfunc_sum(column) %}
    {{ return(adapter.dispatch("aggfunc_sum", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_sum(column) %}
sum({{ column.column_sql }})
{%- endmacro -%}
