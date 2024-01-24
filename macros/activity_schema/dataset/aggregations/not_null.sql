{% macro aggfunc_not_null(column) %}
    {{ return(adapter.dispatch("aggfunc_not_null", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_not_null(column) %}
max({{ column.column_sql }}) is not null
{% endmacro %}
