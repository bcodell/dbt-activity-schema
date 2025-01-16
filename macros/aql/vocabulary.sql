{% macro _activity_verbs() %}

{%- do return(namespace(
    name="activity_verbs",
    select="select",
    append="append",
    aggregate="aggregate",
    include="include",
    verb_list=["select", "append", "aggregate", "include"]
)) -%}

{% endmacro %}

{% macro _relationship_selectors() %}

{%- do return(namespace(
    name="relationship_selectors",
    first="first",
    nth="nth",
    last="last",
    all="all",
    time_spine="time_spine",
    selector_list=["first", "nth", "last", "all", "time_spine"]
)) -%}

{% endmacro %}

{% macro _join_conditions() %}

{%- do return(namespace(
    name="join_conditions",
    before="before",
    between="between",
    after="after",
    ever="ever",
    all="all",
    condition_list=["before", "between", "after", "ever", "all"]
)) -%}

{% endmacro %}

{% macro _valid_query_syntax() %}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set rs = dbt_activity_schema._relationship_selectors() -%}
{%- set jc = dbt_activity_schema._join_conditions() -%}

{%- set relationship_selectors = namespace(
    name="relationship_selectors",
    select=rs.selector_list,
    append=rs.selector_list,
    registry=rs
) -%}

{%- set join_conditions = namespace(
    name="join_conditions",
    append=[jc.before, jc.after, jc.between, jc.ever],
    aggregate=[jc.before, jc.after, jc.between, jc.all],
    registry=jc
) -%}

{%- do return(namespace(
    name="valid_query_syntax",
    relationship_selectors=relationship_selectors,
    join_conditions=join_conditions
)) -%}

{% endmacro %}
