{% macro aggfunc_count_distinct(column) %}
    {{ return(adapter.dispatch("aggfunc_count_distinct", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_count_distinct(column) %}
count(distinct {{ column.column_sql }})
{% endmacro %}
