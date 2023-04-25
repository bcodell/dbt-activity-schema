{% macro aggfunc_min(column) %}
    {{ return(adapter.dispatch("aggfunc_min", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_min(column) %}
min({{ column.column_sql }})
{% endmacro %}
