{% macro aggfunc_max(column) %}
    {{ return(adapter.dispatch("aggfunc_max", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_max(column) %}
max({{ column.column_sql }})
{% endmacro %}
