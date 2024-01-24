{% macro column(
    activity_name,
    stream,
    column_name,
    aggfunc,
    alias=none
) %}

{%- set project_name = model['unique_id'].split(".")[1] -%}
{%- if execute -%}
    {%- set model_prefix = var("dbt_activity_schema").get("streams").get(stream, {}).get("model_prefix", "") -%}
    {%- set activity_node = graph.get("nodes", {}).get("model."~project_name~"."~model_prefix~activity_name, none) -%}

    {%- if activity_node is none -%}
        {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please choose a valid activity - selected '{{activity_name}}'.
Be sure to check aql in dataset column models.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    {%- set activity_node_config = activity_node.get("config", {}) -%}
    {%- set data_types = activity_node_config.get("data_types", {}) -%}
    {%- set customer_column = dbt_activity_schema.customer_column(stream) -%}
    {%- do data_types.update({customer_column: type_string()}) -%}
    {%- do data_types.update(dbt_activity_schema.schema_column_types(stream)) -%}
    {%- if column_name not in data_types.keys() -%}
        {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Column '{{column_name}}' is not registered with a data type
in activity '{{activity_name}}'. Please update the model config appropriately for the activity.
Be sure to check aql in dataset column models.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
    {%- set data_type = data_types.get(column_name, dbt.type_string()) -%}
{%- else -%}
{%- set data_type = dbt.type_string() -%}
{%- endif -%}

{%- set am = dbt_activity_schema._aggregation_map() -%}
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