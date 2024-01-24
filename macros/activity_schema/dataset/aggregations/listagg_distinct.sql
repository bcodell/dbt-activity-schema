{% macro aggfunc_listagg_distinct(column) %}
    {{ return(adapter.dispatch("aggfunc_listagg_distinct", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_listagg_distinct(column) %}
nullif(string_agg(distinct {{ column.column_sql }}, {{dbt_activity_schema.listagg_delimiter()}} order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro duckdb__aggfunc_listagg_distinct(column) %}
nullif(string_agg(distinct {{ column.column_sql }}, {{dbt_activity_schema.listagg_delimiter()}} order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro snowflake__aggfunc_listagg_distinct(column) %}
nullif(listagg(distinct {{ column.column_sql }}, {{dbt_activity_schema.listagg_delimiter()}}) within group (order by {{ column.column_sql }}), '')
{% endmacro %}

{% macro redshift__aggfunc_listagg_distinct(column) %}
nullif(listagg(distinct {{ column.column_sql }}, {{dbt_activity_schema.listagg_delimiter()}}), '')
{% endmacro %}
