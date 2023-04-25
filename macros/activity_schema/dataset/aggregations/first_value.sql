{% macro aggfunc_first_value(column) %}
    {{ return(adapter.dispatch("aggfunc_first_value", "dbt_aql")(column))}}
{% endmacro %}

{% macro default__aggfunc_first_value(column) %}
{%- set primary = dbt_aql.primary() -%}
{%- set req = dbt_aql._required_prefix() -%}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set delimiter = ";.,;" -%}
cast(split_part(
            min(
                cast({{primary}}.{{req}}{{ts}} as {{dbt.type_string()}})
                || {{dbt.string_literal(delimiter)}}
                || cast({{ column.column_sql }} as {{dbt.type_string()}})
            ),
            {{dbt.string_literal(delimiter)}},
            2
        ) as {{column.data_type}})
{%- endmacro -%}

{% macro duckdb__aggfunc_first_value(column) %}
{%- set primary = dbt_aql.primary() -%}
{%- set req = dbt_aql._required_prefix() -%}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set delimiter = ";.,;" -%}
cast(string_split(
            min(
                cast({{primary}}.{{req}}{{ts}} as {{dbt.type_string()}})
                || {{dbt.string_literal(delimiter)}}
                || cast({{ column.column_sql }} as {{dbt.type_string()}})
            ),
            {{dbt.string_literal(delimiter)}}
        )[2] as {{column.data_type}})
{%- endmacro -%}
