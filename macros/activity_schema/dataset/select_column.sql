{% macro select_column(stream, table_alias, column) %}
    {{ return(adapter.dispatch("select_column", "dbt_aql")(stream, table_alias, column))}}
{% endmacro %}

{% macro default__select_column(stream, table_alias, column) %}
{%- set columns = dbt_aql.schema_columns() -%}
{%- do columns.update({"customer": dbt_aql.customer_column(stream)}) -%}
{%- if dbt_aql.anonymous_customer_column(stream) is not none -%}
    {%- do columns.update({"anonymous_customer_id": dbt_aql.anonymous_customer_column(stream)}) -%}
{%- endif -%}
{%- set anonymous = dbt_aql.anonymous_customer_column(stream) -%}
{%- if anonymous is not none -%}
    {%- do columns.update({"anonymous_customer_id": anonymous}) -%}
{%- endif -%}

{%- if column.column_name not in columns.values() -%}
{%- set column_sql -%}
cast(nullif({{dbt_aql.json_extract(table_alias ~ '.' ~ columns.feature_json, column.column_name)}}, '') as {{column.data_type}})
{%- endset -%}
{%- set data_type = column.data_type -%}
{%- else -%}
{%- set column_sql -%}
{{table_alias}}.{{column.column_name}}
{%- endset -%}
{%- set data_type = dbt_aql.schema_column_types().get(column.column_name, dbt.type_string()) -%}
{%- endif -%}
{%- do return(namespace(
    name="selected_column",
    column_sql=column_sql,
    data_type=data_type
)) -%}
{% endmacro %}

{% macro json_extract(json_col, key) %}
    {{ return(adapter.dispatch("json_extract", "dbt_aql")(json_col, key))}}
{% endmacro %}

{# params

key: str
    The name of the key to extract from the feature_json column.
#}

{% macro default__json_extract(json_col, key) -%}
json_extract_path_text({{ json_col }}, {{dbt.string_literal(key) }})
{%- endmacro %}

{% macro bigquery__json_extract(json_col, key) -%}
json_extract({{ json_col }}, {{dbt.string_literal("$."~key) }})
{%- endmacro %}
