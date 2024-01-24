{% macro aggfunc_is_null(column) %}
    {{ return(adapter.dispatch("aggfunc_is_null", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_is_null(column) %}
max({{ column.column_sql }}) is null
{% endmacro %}
