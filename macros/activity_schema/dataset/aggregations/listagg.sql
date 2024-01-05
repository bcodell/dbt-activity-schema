{% macro aggfunc_listagg(column) %}
    {{ return(adapter.dispatch("aggfunc_listagg", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_listagg(column) %}
nullif(string_agg({{ column.column_sql }}, {{dbt_aql.listagg_delimiter()}} order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro duckdb__aggfunc_listagg(column) %}
nullif(string_agg({{ column.column_sql }}, {{dbt_aql.listagg_delimiter()}} order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro snowflake__aggfunc_listagg(column) %}
nullif(listagg({{ column.column_sql }}, {{dbt_aql.listagg_delimiter()}}) within group (order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro redshift__aggfunc_listagg(column) %}
nullif(listagg({{ column.column_sql }}, {{dbt_aql.listagg_delimiter()}}), '')
{% endmacro %}
