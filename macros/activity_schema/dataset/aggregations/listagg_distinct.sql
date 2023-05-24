{% macro aggfunc_listagg_distinct(column) %}
    {{ return(adapter.dispatch("aggfunc_listagg_distinct", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_listagg_distinct(column) %}
listagg(distinct {{ column.column_sql }}, {{dbt_aql._listagg_delimiter()}})
{% endmacro %}
