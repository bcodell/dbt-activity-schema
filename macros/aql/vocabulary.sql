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
    selector_list=["first", "nth", "last", "all"]
)) -%}

{% endmacro %}

{% macro _join_conditions() %}

{%- do return(namespace(
    name="join_conditions",
    before="before",
    between="between",
    between_before="between_before",
    between_after="between_after",
    after="after",
    ever="ever",
    all="all",
    condition_list=["before", "between", "between_before", "between_after", "after", "ever", "all"]
)) -%}

{% endmacro %}

{% macro _valid_query_syntax() %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set rs = dbt_aql._relationship_selectors() -%}
{%- set jc = dbt_aql._join_conditions() -%}

{%- set relationship_selectors = namespace(
    name="relationship_selectors",
    select=rs.selector_list,
    append=rs.selector_list,
    registry=rs
) -%}

{%- set join_conditions = namespace(
    name="join_conditions",
    append=[jc.before, jc.after, jc.between, jc.between_before, jc.between_after, jc.ever],
    aggregate=[jc.before, jc.after, jc.between, jc.between_before, jc.between_after, jc.all],
    registry=jc
) -%}

{%- do return(namespace(
    name="valid_query_syntax",
    relationship_selectors=relationship_selectors,
    join_conditions=join_conditions
)) -%}

{% endmacro %}
