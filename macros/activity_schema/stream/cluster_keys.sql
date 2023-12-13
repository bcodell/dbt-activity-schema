{% macro cluster_keys() %}
    {{ return(adapter.dispatch('cluster_keys', 'dbt_aql')()) }}
{% endmacro %}

{% macro default__cluster_keys() %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- do return(cluster_cols) -%}
{% endmacro %}

{% macro snowflake__cluster_keys() %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- do return(cluster_cols) -%}
{% endmacro %}

{% macro bigquery__cluster_keys() %}
{%- set columns = dbt_aql.schema_columns() -%}
{# assumes that macro is only used in stream models #}
{%- set stream = model.name -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence,
    dbt_aql.customer_column(stream)
] -%}
{%- set partition_cols = {
      "field": columns.ts,
      "data_type": "timestamp",
      "granularity": "month"
} -%}
{%- do return({"cluster_by": cluster_cols, "partition_by": partition_cols}) -%}
{% endmacro %}

{% macro redshift__cluster_keys() %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set cluster_cols = [
    columns.activity,
    columns.ts,
    columns.activity_occurrence
] -%}
{%- do return({"sort": cluster_cols, "dist": "even"}) -%}
{% endmacro %}


{% macro duckdb__cluster_keys() %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- do return(cluster_cols|join(", ")) -%}
{% endmacro %}
