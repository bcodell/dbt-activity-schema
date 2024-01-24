{% macro aggfunc_min(column) %}
    {{ return(adapter.dispatch("aggfunc_min", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_min(column) %}
min({{ column.column_sql }})
{% endmacro %}
