{% macro dataset(aql) %}
    {{ return(adapter.dispatch("dataset", "dbt_aql")(aql)) }}
{% endmacro %}

{% macro default__dataset(aql) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set rs = dbt_aql._relationship_selectors() -%}
{%- set jc = dbt_aql._join_conditions() -%}
{%- set parsed_query = dbt_aql.parse_aql(aql) -%}
{%- set stream = parsed_query.stream -%}
{%- set skip_stream = var("dbt_aql").get("streams", {}).get(stream, {}).get("skip_stream", false) | as_bool -%}
{%- set columns = dbt_aql.schema_columns(stream) -%}
{%- set primary_activity = parsed_query.primary_activity -%}
{%- set primary_activity_alias = dbt_aql.alias_activity(primary_activity, 1) -%}
{%- set primary = dbt_aql.primary() -%}
{%- set joined = dbt_aql.joined() -%}
{%- set joined_activities = parsed_query.joined_activities -%}
{%- set req = dbt_aql._required_prefix() -%}
{%- set fs = dbt_aql._filtered_suffix() -%}
{%- set included_columns = parsed_query.included_dataset_columns -%}
{%- set model_prefix = dbt_aql.get_model_prefix(stream) -%}

{{ dbt_aql._build_dataset(
    stream=stream,
    primary_activity=primary_activity,
    joined_activities=joined_activities,
    included_columns=included_columns
) }}

{% endmacro %}