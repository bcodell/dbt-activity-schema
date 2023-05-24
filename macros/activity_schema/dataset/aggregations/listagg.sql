{% macro aggfunc_listagg(column) %}
    {{ return(adapter.dispatch("aggfunc_listagg", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_listagg(column) %}
listagg({{ column.column_sql }}, {{dbt_aql._listagg_delimiter()}})
{% endmacro %}
