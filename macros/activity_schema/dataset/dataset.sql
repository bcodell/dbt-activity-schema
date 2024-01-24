{% macro dataset(aql) %}
    {{ return(adapter.dispatch("dataset", "dbt_activity_schema")(aql)) }}
{% endmacro %}

{% macro default__dataset(aql) %}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set rs = dbt_activity_schema._relationship_selectors() -%}
{%- set jc = dbt_activity_schema._join_conditions() -%}
{%- set parsed_query = dbt_activity_schema.parse_aql(aql) -%}
{%- set stream = parsed_query.stream -%}
{%- set skip_stream = var("dbt_activity_schema").get("streams", {}).get(stream, {}).get("skip_stream", false) | as_bool -%}
{%- set columns = dbt_activity_schema.schema_columns(stream) -%}
{%- set primary_activity = parsed_query.primary_activity -%}
{%- set primary_activity_alias = dbt_activity_schema.alias_activity(primary_activity, 1) -%}
{%- set primary = dbt_activity_schema.primary() -%}
{%- set joined = dbt_activity_schema.joined() -%}
{%- set joined_activities = parsed_query.joined_activities -%}
{%- set req = dbt_activity_schema._required_prefix() -%}
{%- set fs = dbt_activity_schema._filtered_suffix() -%}
{%- set included_columns = parsed_query.included_dataset_columns -%}
{%- set model_prefix = dbt_activity_schema.get_model_prefix(stream) -%}

{{ dbt_activity_schema._build_dataset(
    stream=stream,
    primary_activity=primary_activity,
    joined_activities=joined_activities,
    included_columns=included_columns
) }}

{% endmacro %}