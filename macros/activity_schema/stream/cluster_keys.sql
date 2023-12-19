{% macro cluster_keys(stream=none) %}
    {{ return(adapter.dispatch('cluster_keys', 'dbt_aql')(stream)) }}
{% endmacro %}

{% macro default__cluster_keys(stream=none) %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- do return(cluster_cols) -%}
{% endmacro %}

{% macro snowflake__cluster_keys(stream=none) %}
{%- set streams = var("dbt_aql").get("streams", {}).keys() -%}
{%- set columns = dbt_aql.schema_columns() -%}

{%- if model.name in streams -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- else -%}
{%- set cluster_cols = [
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- endif -%}
{%- do return(cluster_cols) -%}
{% endmacro %}

{% macro bigquery__cluster_keys(stream=none) %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- set streams = var("dbt_aql").get("streams", {}).keys() -%}

{# throw error if no stream passed as input and model name is not a stream #}
{%- if model.name not in streams and stream == none -%}
    {% set error_message %}
Macro 'cluster_keys' is missing the input 'stream' in model '{{ model.unique_id }}'.
It appears that the macro is being used in an activity model, and Bigquery projects
that use the one table per activity implementation require the stream to be explicitly
passed to the 'cluster_keys' macro in each activity model.
    {% endset %}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- elif model.name in streams -%}
{# cluster stream #}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence,
    dbt_aql.customer_column(model.name)
] -%}
{%- else -%}
{# cluster activity #}
{%- set cluster_cols = [
    columns.activity_occurrence,
    dbt_aql.customer_column(stream)
] -%}
{%- endif -%}
{%- set partition_cols = {
      "field": columns.ts,
      "data_type": "timestamp",
      "granularity": "month"
} -%}
{%- do return({"cluster_by": cluster_cols, "partition_by": partition_cols}) -%}
{% endmacro %}

{% macro redshift__cluster_keys(stream=none) %}
{%- set streams = var("dbt_aql").get("streams", {}).keys() -%}
{%- set columns = dbt_aql.schema_columns() -%}

{%- if model.name in streams -%}
{%- set cluster_cols = [
    columns.activity,
    columns.ts,
    columns.activity_occurrence
] -%}
{%- else -%}
{%- set cluster_cols = [
    columns.ts,
    columns.activity_occurrence
] -%}
{%- endif -%}
{%- do return({"sort": cluster_cols, "dist": "even"}) -%}
{% endmacro %}


{% macro duckdb__cluster_keys(stream=none) %}
{%- set streams = var("dbt_aql").get("streams", {}).keys() -%}
{%- set columns = dbt_aql.schema_columns() -%}

{%- if model.name in streams -%}
{%- set cluster_cols = [
    columns.activity,
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- else -%}
{%- set cluster_cols = [
    columns.activity_occurrence~" in (1, null)",
    columns.activity_repeated_at~" is null",
    "to_date("~columns.ts~")"
] -%}
{%- endif -%}
{%- do return(cluster_cols|join(", ")) -%}
{% endmacro %}
