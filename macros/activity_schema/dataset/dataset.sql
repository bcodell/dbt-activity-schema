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
{%- set columns = dbt_aql.schema_columns() -%}
{%- do columns.update({"customer": dbt_aql.customer_column(stream)}) -%}
{%- if dbt_aql.anonymous_customer_column(stream) is not none -%}
    {%- do columns.update({"anonymous_customer_id": dbt_aql.anonymous_customer_column(stream)}) -%}
{%- endif -%}
{%- set primary_activity = parsed_query.primary_activity -%}
{%- set primary_activity_alias = dbt_aql.alias_activity(primary_activity, 1) -%}
{%- set primary = dbt_aql.primary() -%}
{%- set joined = dbt_aql.joined() -%}
{%- set joined_activities = parsed_query.joined_activities -%}
{%- set req = dbt_aql._required_prefix() -%}
{%- set fs = dbt_aql._filtered_suffix() -%}
{%- set included_columns = parsed_query.included_dataset_columns -%}
{%- set model_prefix = dbt_aql.get_model_prefix(stream) -%}

{% for ic in included_columns %}
    {%- set ic_activity = dbt_aql._build_activity_from_dataset_column(stream, ic.strip()) -%}
    {%- if ic_activity is not none -%}
        {%- do joined_activities.append(ic_activity) -%}
    {%- endif -%}
{% endfor %}

-- depends_on: {{ ref(stream) }}
-- depends_on: {{ ref(primary_activity.model_name) }}

{% for ja in joined_activities %}
-- depends_on: {{ ref(ja.model_name) }}
{% endfor %}

{% for ic in included_columns %}
    {% set ic_stripped = ic.strip() %}
    {% if modules.re.search(model_prefix, ic_stripped) is none %}
        {% set m = model_prefix~ic_stripped %}
-- depends_on: {{ ref(m) }}
    {% else %}
-- depends_on: {{ ref(ic_stripped) }}
    {% endif %}
{% endfor %}


{% set stream_relation = ref(stream) %}

{%- set ja_dict = {} -%}
{%- for ja in joined_activities -%}
    {% if execute %}
        {%- set project_name = model['unique_id'].split(".")[1] -%}
        {%- set activity_node_check = graph.get("nodes", {}).get("model."~project_name~"."~model_prefix~ja.activity_name, None) -%}
        {%- if activity_node_check is none -%}
            {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please choose a valid activity - selected '{{ja.activity_name}}'
            {%- endset -%}
            {{ exceptions.raise_compiler_error(error_message) }}
        {% endif %}
    {% endif %}
    {%- if ja.extra_joins is none -%}
        {%- set extra_join_str = "none" -%}
    {%- else -%}
        {%- set extra_join_str = ja.extra_joins|join("__") -%}
    {%- endif -%}
    {%- if ja.filters is none -%}
        {%- set filter_str = "none" -%}
    {%- else -%}
        {%- set filter_str = ja.filters|join("__") -%}
    {%- endif -%}
    {%- set key = (ja.verb, ja.relationship_selector, ja.join_clause, ja.activity_name, ja.nth, filter_str, extra_join_str) -%}
    {%- if key in ja_dict.keys() -%}
        {%- for col in ja.columns -%}
            {%- do ja_dict[key]["columns"].append(col) -%}
        {%- endfor -%}
    {%- else -%}
        {%- do ja_dict.update({key: ja}) -%}
    {%- endif -%}
{%- endfor -%}

{%- set joined_activities = ja_dict.values() -%}

with
{% if primary_activity.filters is not none %}
{{primary_activity_alias}}{{fs}} as (
    select
        {%- for column in columns.items() %}
        {%- if column[0] not in ['activity_occurrence', 'activity_repeated_at'] %}
        {{primary}}.{{column[1]}},
        {%- endif %}
        {%- endfor %}
        row_number() over (
            partition by {{primary}}.{{columns.customer}}
            order by {{primary}}.{{columns.ts}}, {{primary}}.{{columns.activity_id}}
        ) as {{columns.activity_occurrence}},
        lead({{columns.ts}}) over (
            partition by {{primary}}.{{columns.customer}}
            order by {{primary}}.{{columns.ts}}, {{primary}}.{{columns.activity_id}}
        ) as {{columns.activity_repeated_at}}
    {% if not skip_stream %}
    from {{ stream_relation }} {{primary}}
    {% else %}
    from {{ ref(primary_activity.model_name) }} {{primary}}
    {% endif %}
    where true
        {% if not skip_stream %}
        and {{primary}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, primary_activity.activity_name)}}
        {% endif %}
        {% for f in primary_activity.filters %}
        {%- set f_formatted = f.format(primary=primary, joined=joined, **columns) %}
        and {{f_formatted}}
        {%- endfor %}
),
{% endif %}
{{primary_activity_alias}} as (
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
    from {% if primary_activity.filters is none %}{% if not skip_stream %}{{ stream_relation }}{% else %}{{ ref(primary_activity.model_name) }}{% endif %}{% else %}{{primary_activity_alias}}{{fs}}{% endif %} as {{primary}}
    where true
        {% if not skip_stream %}
        and {{primary}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, primary_activity.activity_name)}}
        {% endif %}
        and {{ primary_activity.relationship_clause }}
){% if joined_activities|length > 0 %},{% endif %}
{% for ja in joined_activities %}

{# cte below only applies to filtered append activities since activity occurrence and next activity need to be recomputed for use in the join #}
{% if ja.filters is not none and ja.verb == av.append %}
{{dbt_aql.alias_activity(ja, loop.index)}}{{fs}} as (
    select
        {%- for column in columns.items() %}
        {%- if column[0] not in ['activity_occurrence', 'activity_repeated_at'] %}
        {{joined}}.{{column[1]}},
        {%- endif %}
        {%- endfor %}
        row_number() over (
            partition by {{joined}}.{{columns.customer}}
            order by {{joined}}.{{columns.ts}}, {{joined}}.{{columns.activity_id}}
        ) as {{columns.activity_occurrence}},
        lead({{columns.ts}}) over (
            partition by {{joined}}.{{columns.customer}}
            order by {{joined}}.{{columns.ts}}, {{joined}}.{{columns.activity_id}}
        ) as {{columns.activity_repeated_at}}
    {% if not skip_stream %}
    from {{ stream_relation }} {{joined}}
    {% else %}
    from {{ ref(ja.model_name) }} {{joined}}
    {% endif %}
    where true
        {% if not skip_stream %}
        and {{joined}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, ja.activity_name)}}
        {% endif %}
        {% for f in ja.filters %}
        {%- set f_formatted = f.format(primary=primary, joined=joined, **columns) %}
        and {{f_formatted}}
        {%- endfor %}
),
{% endif %}
{{ dbt_aql.alias_activity(ja, loop.index) }} as (
    {% if (ja.verb, ja.join_condition) != (av.aggregate, jc.all) %}
    select
        {{primary}}.{{req}}{{columns.activity_id}},
        {%- for column in ja.columns -%}
        {%- set parsed_col = dbt_aql.select_column(stream, joined, column) %}
        {{ column.aggfunc(parsed_col) }} as {{ column.alias }}{% if not loop.last -%},{%- endif -%}
        {%- endfor %}
    from {{primary_activity_alias}} as {{primary}}
    left join {% if ja.filters is not none and ja.verb == av.append %}{{dbt_aql.alias_activity(ja, loop.index)}}{{fs}}{% else %}{% if not skip_stream %}{{ stream_relation }}{% else %}{{ ref(ja.model_name) }}{% endif %}{% endif %} {{joined}}
        -- filter joined activity first to improve query performance
        on true
        {% if not skip_stream %}
        and {{joined}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, ja.activity_name)}}
        {% endif %}
        {%- if ja.relationship_clause is not none %}
        and {{ ja.relationship_clause }}
        {%- endif %}
        and {{primary}}.{{req}}{{columns.customer}} = {{joined}}.{{columns.customer}}
        and {{ ja.join_clause }}
        {%- if ja.extra_joins is not none %}
        {%- for ej in ja.extra_joins %}
        {%- set ej_formatted = ej.format(primary=primary, joined=joined, **columns) %}
        and {{ej_formatted}}
        {%- endfor %}
        {%- endif %}
        {% if ja.filters is not none and ja.verb == av.aggregate %}
        {% for f in ja.filters %}
        {%- set f_formatted = f.format(primary=primary, joined=joined, **columns) %}
        and {{f_formatted}}
        {%- endfor %}
        {% endif %}
    group by 1
    {% else %}
    -- special join case for aggregate all to improve performance
    select
        {{joined}}.{{columns.customer}},
        {%- for column in ja.columns %}
            {%- set parsed_col = dbt_aql.select_column(stream, joined, column) -%}
            {{ column.aggfunc(parsed_col) }} as {{ column.alias }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    {% if not skip_stream %}
    from {{ ref(stream) }} {{joined}}
    {% else %}
    from {{ ref(ja.model_name) }} {{joined}}
    {% endif %}
    where {{joined}}.{{columns.activity}} = {{dbt_aql.clean_activity_name(stream, ja.activity_name)}}
    {% if ja.filters is not none %}
        {%- for f in ja.filters %}
        {%- set f_formatted = f.format(primary=primary, joined=joined, **columns) %}
        and {{f_formatted}}
        {%- endfor %}
    {% endif %}
    group by 1
    {% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}
select
    {%- for column in primary_activity.columns %}
    {{primary}}.{{column.alias}}{% if loop.last and joined_activities|length == 0 -%}{% else -%},{%- endif -%}
    {%- endfor %}
    {%- for ja in joined_activities -%}
    {%- set ja_alias = dbt_aql.alias_activity(ja, loop.index) -%}
    {%- set ja_loop_last = loop.last %}
    {%- for column in ja.columns %}
    {%- if column.zero_fill %}
    {{ dbt_aql.zero_fill(ja_alias~'.'~column.alias) }} as {{column.alias}}{% if ja_loop_last and loop.last -%}{%- else %},{%- endif -%}
    {%- else %}
    {{ja_alias}}.{{column.alias}} as {{column.alias}}{% if ja_loop_last and loop.last -%}{%- else %},{%- endif -%}
    {%- endif %}
    {%- endfor %}
    {%- endfor %}
from {{ primary_activity_alias }} as {{primary}}
{%- for ja in joined_activities %}
{%- set joined_alias = dbt_aql.alias_activity(ja, loop.index) %}
left join {{ joined_alias }}
{%- if (ja.verb, ja.join_condition) != (av.aggregate, jc.all) %}
    on {{primary}}.{{req}}{{columns.activity_id}} = {{joined_alias}}.{{req}}{{columns.activity_id}}
{%- else %}
    -- join on customer column for aggregate all
    on {{primary}}.{{req}}{{columns.customer}} = {{joined_alias}}.{{columns.customer}}
{%- endif %}
{%- endfor %}

{% endmacro %}