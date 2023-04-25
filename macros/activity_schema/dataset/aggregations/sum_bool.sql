{% macro aggfunc_sum_bool(column) %}
    {{ return(adapter.dispatch("aggfunc_sum_bool", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_sum_bool(column) %}
sum(cast({{ column.column_sql }} as {{type_boolean()}}))
{% endmacro %}
