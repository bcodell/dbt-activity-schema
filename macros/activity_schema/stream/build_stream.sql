{% macro build_stream(activity_list) %}
{{ return(adapter.dispatch('build_stream', 'dbt_activity_schema')(activity_list)) }}
{% endmacro %}

{% macro default__build_stream(activity_list) %}


{% for activity in activity_list %}
{{"-- depends_on: "~activity}}
{% endfor %}

{%- set skip_stream = var("dbt_activity_schema").get("streams", {}).get(model.name, {}).get("skip_stream", false) | as_bool -%}

{%- set columns = dbt_activity_schema.schema_columns(model.name) -%}
{%- set schema_column_types = dbt_activity_schema.schema_column_types(model.name) -%}

{% if not skip_stream %}
{% for activity in activity_list %}
select *
from {{activity}}
{% if is_incremental() %}
where {{columns.ts}} > (select coalesce(max({{columns.ts}}), {{dbt.safe_cast(dbt.string_literal('0001-01-01'), dbt.type_timestamp())}}) from {{this}} where {{columns.activity}} = {{dbt_activity_schema.clean_activity_name(model.name, activity.name)}})
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}

{% else %}
select
    {% for key in columns.keys() %}
    {% if not loop.first %}, {% endif %} cast(null as {{schema_column_types.get(key, type_string())}}) as {{columns.get(key)}}
    {% endfor %}
{% endif %}
{% endmacro %}
