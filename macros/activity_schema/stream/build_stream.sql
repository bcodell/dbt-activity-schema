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
{% if not loop.last %}
union all
{% endif %}
{% endfor %}


{% endmacro %}
