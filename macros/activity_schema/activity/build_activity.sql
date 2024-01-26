{#
build_activity: Compiles a final select statement in a standardized format so that all activity models can be unioned together in the final activity stream model. For each activity transformation model, a final CTE should be compiled containing the primary customer id used in the activity stream, a timestamp column representing when the timestamp occurred, and all of the static attributes associated with the activity, then this macro should be applied after that CTE. No aggregations or transformations beyond basic selecting and column aliasing will occur in this macro.
    - cte: string; the name of the last CTE in the query containing all relevant columns to compile the activity model
#}

{% macro build_activity(cte, unique_id_column=none, null_columns=[]) %}
{{ return(adapter.dispatch('build_activity', 'dbt_activity_schema')(cte, unique_id_column, null_columns)) }}
{% endmacro %}


{% macro default__build_activity(cte, unique_id_column=none, null_columns=[]) %}

{%- for nc in null_columns -%}
    {%- set accepted_values = ["anonymous_customer_id", "revenue_impact", "link"] -%}
    {%- if nc not in accepted_values -%}
        {% set accepted_values_str = accepted_values|join(", ") %}
        {% set error_message %}
Passed invalid value to `null_columns` argument in model '{{ model.unique_id }}'.
Accepted values are one of {{accepted_values_str}}, but received '{{nc}}'
        {% endset %}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{%- endfor -%}

{% if execute %}
    {%- set data_types = config.get("data_types", none) -%}
    {%- set stream = config.require("stream") -%}
{% else %}
    {%- set stream = "stream" -%}
    {%- set data_types = none -%}
{% endif %}


{%- if execute -%}
    {%- set columns = dbt_activity_schema.schema_columns(stream=stream) -%}
{%- else -%}
    {%- set columns = dbt_activity_schema.schema_columns() -%}
{%- endif -%}

{%- set model_name_raw = model.name -%}
{% set model_name = dbt_activity_schema.clean_activity_name(stream, model.name) %}


{%- set ts = columns.ts -%}
{%- set surrogate_key_fields = [columns.customer, columns.ts, model_name] -%}
{%- if unique_id_column is not none -%}
{%- do surrogate_key_fields.append(unique_id_column) -%}
{%- endif -%}

{%- set surrogate_key_statement -%}
cast({{ dbt_activity_schema.generate_activity_id(surrogate_key_fields) }} as {{dbt.type_string()}})
{%- endset -%}

{%- if execute -%}
    {%- set schema_column_types = dbt_activity_schema.schema_column_types(stream) -%}
{%- else -%}
    {%- set schema_column_types = dbt_activity_schema.schema_column_types() -%}
{%- endif -%}


select
    cast({{surrogate_key_statement}} as {{schema_column_types[columns.activity_id]}}) as {{columns.activity_id}}
    , cast({{columns.customer}} as {{dbt.type_string()}}) as {{columns.customer}}
    {% if columns.anonymous_customer_id is defined %}
        {% if "anonymous_customer_id" not in null_columns %}
    , cast({{columns.anonymous_customer_id}} as {{dbt.type_string()}}) as {{columns.anonymous_customer_id}}
        {% else %}
    , cast(null as {{dbt.type_string()}}) as {{columns.anonymous_customer_id}}
        {% endif %}
    {% endif %}
    , cast({{model_name}} as {{schema_column_types[columns.activity]}}) as {{columns.activity}}
    , cast({{columns.ts}} as {{schema_column_types[columns.ts]}}) as {{columns.ts}}
    {% if columns.revenue_impact is defined %}
        {% if "revenue_impact" not in null_columns %}
    , cast({{columns.revenue_impact}} as {{schema_column_types[columns.revenue_impact]}}) as {{columns.revenue_impact}}
        {% else %}
    , cast(null as {{schema_column_types[columns.revenue_impact]}}) as {{columns.revenue_impact}}
        {% endif %}
    {% endif %}
    {% if columns.link is defined %}
        {% if "link" not in null_columns %}
    , cast({{columns.link}} as {{schema_column_types[columns.link]}}) as {{columns.link}}
        {% else %}
    , cast(null as {{schema_column_types[columns.link]}}) as {{columns.link}}
        {% endif %}
    {% endif %}
    , {{ dbt_activity_schema.build_json(data_types) }} as {{columns.feature_json}}
    , row_number() over (
        {% if columns.anonymous_customer_id is not defined or 'anonymous_customer_id' in null_columns %}
        partition by {{columns.customer}}
        {% else %}
        partition by coalesce({{columns.customer}}, {{columns.anonymous_customer_id}})
        {% endif %}
        order by {{columns.ts}}, {{surrogate_key_statement}}
    ) as activity_occurrence
    , lead(cast({{columns.ts}} as {{dbt.type_timestamp()}})) over (
        {% if columns.anonymous_customer_id is not defined or 'anonymous_customer_id' in null_columns %}
        partition by {{columns.customer}}
        {% else %}
        partition by coalesce({{columns.customer}}, {{columns.anonymous_customer_id}})
        {% endif %}
        order by {{columns.ts}}, {{surrogate_key_statement}}
    ) as activity_repeated_at
from {{cte}}

{% endmacro %}
