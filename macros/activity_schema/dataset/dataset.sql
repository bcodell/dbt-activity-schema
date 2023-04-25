{% macro dataset(aql) %}
    {{ return(adapter.dispatch("dataset", "dbt_aql")(aql)) }}
{% endmacro %}

{% macro default__dataset(aql) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set rs = dbt_aql._relationship_selectors() -%}
{%- set jc = dbt_aql._join_conditions() -%}
{%- set parsed_query = dbt_aql.parse_aql(aql) -%}
{%- set stream = parsed_query.stream -%}
{%- set columns = dbt_aql.schema_columns() -%}
{%- do columns.update({"customer": dbt_aql.customer_column(stream)}) -%}
{%- if dbt_aql.anonymous_customer_column(stream) is not none -%}
    {%- do columns.update({"anonymous_customer_id": dbt_aql.anonymous_customer_column(stream)}) -%}
{%- endif -%}
{%- set primary_activity = parsed_query.primary_activity -%}
{%- set primary_activity_alias = dbt_aql.alias_activity(primary_activity) -%}
{%- set primary = dbt_aql.primary() -%}
{%- set joined = dbt_aql.joined() -%}
{%- set joined_activities = parsed_query.joined_activities -%}
{%- set req = dbt_aql._required_prefix() -%}

-- depends_on: {{ ref(stream) }}

with {{primary_activity_alias}} as (
    select
        -- required columns to use for joins
        {{primary}}.{{columns.activity_id}} as {{req}}{{columns.activity_id}},
        {{primary}}.{{columns.customer}} as {{req}}{{columns.customer}},
        {{primary}}.{{columns.ts}} as {{req}}{{columns.ts}},
        {{primary}}.{{columns.activity_occurrence}} as {{req}}{{columns.activity_occurrence}},
        {{primary}}.{{columns.activity_repeated_at}} as {{req}}{{columns.activity_repeated_at}},
        {%- for column in primary_activity.columns %}
        {{ dbt_aql.select_column(stream, primary, column).column_sql }} as {{column.alias}}{% if not loop.last -%},{%- endif -%}
        {%- endfor %}
    from {{ ref(stream) }} as {{primary}}
    where {{primary}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, primary_activity.activity_name)}}
        and {{ primary_activity.relationship_clause }}
){% if joined_activities|length > 0 %},{% endif %}
{% for ja in joined_activities %}
{{ dbt_aql.alias_activity(ja) }} as (
    {% if (ja.verb, ja.join_condition) != (av.aggregate, jc.all) %}
    select
        {{primary}}.{{req}}{{columns.activity_id}},
        {%- for column in ja.columns -%}
        {%- set parsed_col = dbt_aql.select_column(stream, joined, column) %}
        {{ column.aggfunc(parsed_col) }} as {{ column.alias }}{% if not loop.last -%},{%- endif -%}
        {%- endfor %}
    from {{dbt_aql.alias_activity(primary_activity)}} as {{primary}}
    left join {{ ref(stream) }} {{joined}}
        -- filter joined activity first to improve query performance
        on {{joined}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, ja.activity_name)}}
        {%- if ja.relationship_clause is not none %}
        and {{ ja.relationship_clause }}
        {%- endif %}
        and {{primary}}.{{req}}{{columns.customer}} = {{joined}}.{{columns.customer}}
        {%- if ja.verb == av.aggregate %}
        and {{ ja.join_clause }}
        {%- endif -%}
        {%- if ja.extra_joins is not none %}
        {%- for ej in ja.extra_joins %}
        {%- set ej_formatted = ej.format(primary=primary, joined=joined, **columns) %}
        and {{ej_formatted}}
        {%- endfor %}
        {%- endif %}
    group by 1
    {% else %}
    -- special join case for aggregate all to improve performance
    select
        {{joined}}.{{columns.customer}},
        {%- for column in ja.columns %}
            {%- set parsed_col = dbt_aql.select_column(stream, joined, column) -%}
            {{ column.aggfunc(parsed_col) }} as {{ column.alias }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref(stream) }} {{joined}}
    where {{joined}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, ja.activity_name)}}
    group by 1
    {% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}
select
    {%- for column in primary_activity.columns %}
    {{primary}}.{{column.alias}}{% if loop.last and joined_activities|length == 0 -%}{% else -%},{%- endif -%}
    {%- endfor %}
    {%- for ja in joined_activities -%}
    {%- set ja_loop_last = loop.last %}
    {%- for column in ja.columns %}
    {%- if column.zero_fill %}
    {{ dbt_aql.zero_fill(dbt_aql.alias_activity(ja)~'.'~column.alias) }} as {{column.alias}}{% if ja_loop_last and loop.last -%}{%- else %},{%- endif -%}
    {%- else %}
    {{dbt_aql.alias_activity(ja)}}.{{column.alias}} as {{column.alias}}{% if ja_loop_last and loop.last -%}{%- else %},{%- endif -%}
    {%- endif %}
    {%- endfor %}
    {%- endfor %}
from {{ primary_activity_alias }} as {{primary}}
{%- for ja in joined_activities %}
{%- set joined_alias = dbt_aql.alias_activity(ja) %}
left join {{ joined_alias }}
{%- if (ja.verb, ja.join_condition) != (av.aggregate, jc.all) %}
    on {{primary}}.{{req}}{{columns.activity_id}} = {{joined_alias}}.{{req}}{{columns.activity_id}}
{%- else %}
    -- join on customer column for aggregate all
    on {{primary}}.{{req}}{{columns.customer}} = {{joined_alias}}.{{columns.customer}}
{%- endif %}
{%- endfor %}

{% endmacro %}