{% macro select_column(stream, table_alias, column) %}
    {{ return(adapter.dispatch("select_column", "dbt_activity_schema")(stream, table_alias, column))}}
{% endmacro %}

{% macro default__select_column(stream, table_alias, column) %}
{%- set columns = dbt_activity_schema.schema_columns(stream) -%}

{%- if column.column_name not in columns.values() -%}
{%- set column_sql -%}
cast(nullif({{dbt_activity_schema.json_extract(table_alias ~ '.' ~ columns.feature_json, column.column_name)}}, '') as {{column.data_type}})
{%- endset -%}
{%- set data_type = column.data_type -%}
{%- else -%}
{%- set column_sql -%}
{{table_alias}}.{{column.column_name}}
{%- endset -%}
{%- set data_type = dbt_activity_schema.schema_column_types(stream).get(column.column_name, dbt.type_string()) -%}
{%- endif -%}
{%- do return(namespace(
    name="selected_column",
    column_sql=column_sql,
    data_type=data_type
)) -%}
{% endmacro %}

{% macro json_extract(json_col, key) %}
    {{ return(adapter.dispatch("json_extract", "dbt_activity_schema")(json_col, key))}}
{% endmacro %}

{# params

key: str
    The name of the key to extract from the feature_json column.
#}

{% macro default__json_extract(json_col, key) -%}
json_extract_path_text({{ json_col }}, {{dbt.string_literal(key) }})
{%- endmacro %}

{% macro bigquery__json_extract(json_col, key) -%}
json_value({{ json_col }}.{{key}})
{%- endmacro %}

{%- macro snowflake__json_extract(json_col, key) -%}
to_varchar(get_path({{json_col}}, '{{key}}'))
{%- endmacro -%}
