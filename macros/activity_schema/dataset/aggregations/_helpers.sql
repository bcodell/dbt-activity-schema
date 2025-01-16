{% macro listagg_delimiter() %}
    {{ return(adapter.dispatch("listagg_delimiter", "dbt_activity_schema")())}}
{% endmacro %}

{% macro default__listagg_delimiter() %}
{%- do return(dbt.string_literal("\n")) -%}
{% endmacro %}

{% macro type_boolean() %}
    {{ return(adapter.dispatch("type_boolean", "dbt_activity_schema")())}}
{% endmacro %}

{% macro default__type_boolean() %}
{%- do return("boolean") -%}
{% endmacro %}

{% macro bigquery__type_boolean() %}
{%- do return("bool") -%}
{% endmacro %}


{% macro type_json() %}
    {{ return(adapter.dispatch("type_json", "dbt_activity_schema")())}}
{% endmacro %}

{% macro default__type_json() %}
{%- do return("json") -%}
{% endmacro %}

{% macro snowflake__type_json() %}
{%- do return("object") -%}
{% endmacro %}

{% macro bigquery__type_json() %}
{%- do return("json") -%}
{% endmacro %}

{% macro end_period_expression(end_period, ts_col) %}
{%- if end_period == 'current' -%}
{{dbt_activity_schema.current_timestamp()}}
{%- elif end_period == 'max' -%}
max({{dbt_activity_schema.primary()}}.{{ts_col}})
{%- else -%}
{%- endif -%}
{% endmacro %}


{% macro current_timestamp() %}
    {{ return(adapter.dispatch("current_timestamp", "dbt_activity_schema")())}}
{% endmacro %}

{% macro default__current_timestamp() %}
current_timestamp
{% endmacro %}

{% macro bigquery__current_timestamp() %}
current_timestamp()
{% endmacro %}


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
date_add({{ts}}, interval {{periods}} {{interval}})
{% endmacro %}


{% macro to_timestamp(ts) %}
    {{ return(adapter.dispatch("to_timestamp", "dbt_activity_schema")(ts))}}
{% endmacro %}

{% macro default__to_timestamp(ts) %}
{{ts}}::timestamp
{% endmacro %}

{% macro bigquery__to_timestamp(ts) %}
timestamp({{ts}})
{% endmacro %}



{% macro date_trunc(period, ts) %}
    {{ return(adapter.dispatch("date_trunc", "dbt_activity_schema")(period, ts))}}
{% endmacro %}

{% macro default__date_trunc(period, ts) %}
date_trunc('{{period}}', {{ts}})
{% endmacro %}

{% macro bigquery__date_trunc(period, ts) %}
date_trunc({{ts}}, {{period}})
{% endmacro %}


{% macro date_diff(period, start_ts, end_ts) %}
    {{ return(adapter.dispatch("date_diff", "dbt_activity_schema")(period, start_ts, end_ts))}}
{% endmacro %}

{% macro default__date_diff(period, start_ts, end_ts) %}
datediff('{{period}}', {{start_ts}}, {{end_ts}})
{% endmacro %}

{% macro duckdb__date_diff(period, start_ts, end_ts) %}
date_diff('{{period}}', {{start_ts}}, {{end_ts}})
{% endmacro %}

{% macro bigquery__date_diff(period, start_ts, end_ts) %}
date_diff({{end_ts}}, {{start_ts}}, {{period}})
{% endmacro %}


{% macro md5(expr) %}
    {{ return(adapter.dispatch("md5", "dbt_activity_schema")(expr))}}
{% endmacro %}

{% macro default__md5(expr) %}
md5({{expr}})
{% endmacro %}

{% macro bigquery__md5(expr) %}
to_hex(md5({{expr}}))
{% endmacro %}
