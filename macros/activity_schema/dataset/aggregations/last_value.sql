{% macro aggfunc_last_value(column) %}
    {{ return(adapter.dispatch("aggfunc_last_value", "dbt_activity_schema")(column))}}
{% endmacro %}

{% macro default__aggfunc_last_value(column) %}
{%- set joined = dbt_activity_schema.joined() -%}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set delimiter = ";.,;" -%}
cast(split_part(
            max(
                cast({{joined}}.{{ts}} as {{dbt.type_string()}})
                || {{dbt.string_literal(delimiter)}}
                || cast({{ column.column_sql }} as {{dbt.type_string()}})
            ),
            {{dbt.string_literal(delimiter)}},
            2
        ) as {{column.data_type}})
{%- endmacro -%}

{% macro duckdb__aggfunc_last_value(column) %}
{%- set joined = dbt_activity_schema.joined() -%}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set delimiter = ";.,;" -%}
cast(string_split(
            max(
                cast({{joined}}.{{ts}} as {{dbt.type_string()}})
                || {{dbt.string_literal(delimiter)}}
                || cast({{ column.column_sql }} as {{dbt.type_string()}})
            ),
            {{dbt.string_literal(delimiter)}}
        )[2] as {{column.data_type}})
{%- endmacro -%}

{% macro snowflake__aggfunc_last_value(column) %}
{%- set joined = dbt_activity_schema.joined() -%}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
max_by({{column.column_sql}}, {{joined}}.{{ts}})
{% endmacro %}

{% macro bigquery__aggfunc_last_value(column) %}
{%- set joined = dbt_activity_schema.joined() -%}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set delimiter = ";.,;" -%}
cast(split(
            max(
                cast({{joined}}.{{ts}} as {{dbt.type_string()}})
                || {{dbt.string_literal(delimiter)}}
                || cast({{ column.column_sql }} as {{dbt.type_string()}})
            ),
            {{dbt.string_literal(delimiter)}}
        )[safe_offset(1)] as {{column.data_type}})
{%- endmacro -%}
