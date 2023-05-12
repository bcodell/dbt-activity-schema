{% macro build_stream(activity_list) %}
{{ return(adapter.dispatch('build_stream', 'dbt_aql')(activity_list)) }}
{% endmacro %}

{% macro default__build_stream(activity_list) %}


{% for activity in activity_list %}
{{"-- depends_on: "~activity}}
{% endfor %}

{% for activity in activity_list %}
select *
from {{activity}}
{% if is_incremental() %}
{% set columns = dbt_aql.schema_columns() %}
where {{columns.ts}} > (select coalesce(max({{columns.ts}}), {{dbt.safe_cast(dbt.string_literal('0001-01-01'), dbt.type_timestamp())}}) from {{this}} where {{columns.activity}} = {{dbt_aql.clean_activity_name(model.name, activity.name)}})
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}


{% endmacro %}
