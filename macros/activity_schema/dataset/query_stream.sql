{% macro query_stream(stream, primary_activity, joined_activities=[], included_columns=[]) %}

{#
    stream: string
        the stream being queried
    primary_activity: primary_activity
        the primary activity in the dataset
    joined_activities: [list[appended_activity, aggregated_activity]] (optional)
        the joined activities in the dataset
    included_columns: [list[ref]] (optional)
        the dataset_column materialized models to include in the dataset
#}

{%- set rs = dbt_activity_schema._relationship_selectors() -%}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set am = dbt_activity_schema._aggregation_map() -%}
{% set stream_name = stream %}
{%- set model_prefix = dbt_activity_schema.get_model_prefix(stream_name) -%}
{%- set primary_activity_name = primary_activity.activity_name|replace(model_prefix, '') -%}
{%- set pcl = [] -%}
{%- for pc in primary_activity.columns -%}
    {%- do pcl.append(dbt_activity_schema.column(
            activity_name=primary_activity_name,
            stream=stream_name,
            column_name=pc.column_name,
            aggfunc=none,
            alias=pc.alias
    )) -%}
{%- endfor -%}

{% set pa = dbt_activity_schema.activity(
    activity_name=primary_activity_name,
    stream=stream_name,
    verb=primary_activity.verb,
    join_condition=primary_activity.join_condition,
    relationship_selector=primary_activity.relationship_selector,
    columns=primary_activity.columns,
    nth=primary_activity.nth,
    filters=primary_activity.filters,
    extra_joins=primary_activity.extra_joins
) %}

{% set ja = [] %}
{% for j in joined_activities %}
    {%- set joined_activity_name = j.activity_name|replace(model_prefix, '') -%}
    {% set co = [] %}
    {% for c in j.columns %}
        {%- if j.verb == av.append -%}
            {%- if j.relationship_selector == rs.first -%}
                {%- set aggfunc = am.first_value -%}
            {%- elif j.relationship_selector == rs.last -%}
                {%- set aggfunc = am.last_value -%}
            {%- elif j.relationship_selector == rs.nth -%}
                {# arbitrary for nth #}
                {%- set aggfunc = am.last_value -%}
            {%- endif -%}
        {%- else -%}
            {%- set aggfunc = am[c.aggfunc] -%}
        {%- endif -%}
        {% do co.append(dbt_activity_schema.column(
            activity_name=joined_activity_name,
            stream=stream_name,
            column_name=c.column_name,
            aggfunc=aggfunc,
            alias=c.alias
        )) %}
    {% endfor %}
{% set joined_activity = dbt_activity_schema.activity(
    activity_name=joined_activity_name,
    stream=stream_name,
    verb=j.verb,
    join_condition=j.join_condition,
    relationship_selector=j.relationship_selector,
    columns=co,
    nth=j.nth,
    filters=j.filters,
    extra_joins=j.extra_joins
) %}
{% do ja.append(joined_activity) %}
{% endfor %}

{% set ic = [] %}
{% for i in included_columns %}
{% do ic.append(i) %}
{% endfor %}


{{ dbt_activity_schema._build_dataset(
    stream=stream_name,
    primary_activity=pa,
    joined_activities=ja,
    included_columns=ic
) }}


{% endmacro %}