{% macro aggfunc_max(column) %}
    {{ return(adapter.dispatch("aggfunc_max", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_max(column) %}
max({{ column.column_sql }})
{% endmacro %}
