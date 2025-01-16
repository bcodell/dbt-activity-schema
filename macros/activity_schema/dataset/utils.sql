{% macro dateadd(interval, periods, ts) %}
    {{ return(adapter.dispatch('dateadd', 'dbt_activity_schema')(interval, periods, ts)) }}
{% endmacro %}

{% macro default__dateadd(interval, periods, ts) %}
dateadd({{interval}}, {{periods}}, {{ts}})
{% endmacro %}

{% macro duckdb__dateadd(interval, periods, ts) %}
date_add({{ts}}, {{periods}} * interval 1 {{interval}})
{% endmacro %}

{% macro snowflake__dateadd(interval, periods, ts) %}
dateadd({{interval}}, {{periods}}, {{ts}})
{% endmacro %}

{% macro redshift__dateadd(interval, periods, ts) %}
dateadd({{interval}}, {{periods}}, {{ts}})
{% endmacro %}

{% macro bigquery__dateadd(interval, periods, ts) %}
date_add({{ts}}, {{periods}} * interval 1 {{interval}})
{% endmacro %}



SELECT DATE_ADD(DATE('2025-01-01'), INTERVAL 7 DAY) AS new_date;
