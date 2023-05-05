{% macro column(
    activity_name,
    stream,
    column_name,
    aggfunc,
    alias=none
) %}

{%- set project_name = model['unique_id'].split(".")[1] -%}
{%- if execute -%}
    {%- set model_prefix = var("dbt_aql").get("streams").get(stream, {}).get("model_prefix", "") -%}
    {%- set activity_node_config = graph.get("nodes", {}).get("model."~project_name~"."~model_prefix~activity_name, {}).get("config", {}) -%}
    {%- set data_type = activity_node_config.get("data_types", {}).get(column_name, dbt.type_string()) -%}
{%- else -%}
{%- set data_type = dbt.type_string() -%}
{%- endif -%}

{%- set am = dbt_aql._aggregation_map() -%}
{%- if aggfunc is not none -%}
    {%- set zero_fill = aggfunc in am.zero_fill_aggregations -%}
{%- else -%}
    {%- set zero_fill = false -%}
{%- endif -%}

{%- do return(namespace(
    name="column",
    activity_name=activity_name,
    column_name=column_name,
    alias=alias,
    aggfunc=aggfunc,
    zero_fill=zero_fill,
    data_type=data_type
)) -%}

{% endmacro %}