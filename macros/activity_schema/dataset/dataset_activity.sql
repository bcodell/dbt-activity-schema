{% macro dc(
    column_name,
    aggfunc=none,
    alias=none
) %}

{% do return(namespace(
    name="dataset_column",
    column_name=column_name,
    alias=alias,
    aggfunc=aggfunc
)) %}

{% endmacro %}

{% macro primary_activity(
    activity,
    columns,
    relationship_selector,
    nth=none,
    filters=none,
    extra_joins=none
)
%}
{#
    activity: str
        the activity to use
    columns: list[dc]
        a list of columns to use, each passed as a dc macro object
    relationship_selector: string
        the relationship selector to use.
        valid options are ['first', 'nth', 'last', 'all']
    nth: int (optional)
        the nth instance of the activity to use.
        only valid relationship selector is 'nth'
    filters: list[string] (optional)
        the set of filters to apply to subset the activity.
    extra_joins: list[string] (optional)
        the set of additional join conditions to apply when joining the activity to the primary.
        only valid for joined activities
#}

{% set ns = namespace(
    name="dataset_activity",
    activity_name=activity,
    columns=columns,
    verb="select",
    relationship_selector=relationship_selector,
    nth=nth,
    join_condition=none,
    filters=filters,
    extra_joins=extra_joins
) %}


{% do return(namespace(
    name="dataset_activity",
    activity_name=activity,
    columns=columns,
    verb="select",
    relationship_selector=relationship_selector,
    nth=nth,
    join_condition=none,
    filters=filters,
    extra_joins=extra_joins
)) %}

{% endmacro %}



{% macro appended_activity(
    activity,
    columns,
    relationship_selector=none,
    nth=none,
    join_condition=none,
    filters=none,
    extra_joins=none
) %}

{#
    activity: str
        the activity to use
    columns: list[dc]
        a list of columns to use, each passed as a dc macro object
    relationship_selector: string
        the relationship selector to use.
        valid options are ['first', 'nth', 'last', 'all']
    nth: int (optional)
        the nth instance of the activity to use.
        only valid relationship selector is 'nth'
    join_condition: string (optional)
        the join condition to use. only valid for joined activities.
        valid options are ["before", "between", "after", "ever", "all"]
    filters: list[string] (optional)
        the set of filters to apply to subset the activity.
    extra_joins: list[string] (optional)
        the set of additional join conditions to apply when joining the activity to the primary.
#}

{% do return(namespace(
    name="dataset_activity",
    activity_name=activity,
    columns=columns,
    verb="append",
    relationship_selector=relationship_selector,
    nth=nth,
    join_condition=join_condition,
    filters=filters,
    extra_joins=extra_joins
)) %}

{% endmacro %}

{% macro aggregated_activity(
    activity,
    columns,
    join_condition=none,
    filters=none,
    extra_joins=none
) %}

{#
    activity: str
        the activity to use
    columns: list[dc]
        a list of columns to use, each passed as a dc macro object
    join_condition: string
        the join condition to use. only valid for joined activities.
        valid options are ["before", "between", "after", "ever", "all"]
    filters: list[string] (optional)
        the set of filters to apply to subset the activity.
    extra_joins: list[string] (optional)
        the set of additional join conditions to apply when joining the activity to the primary.
        not valid when join condition is 'all'
#}

{% do return(namespace(
    name="dataset_activity",
    activity_name=activity,
    columns=columns,
    verb="aggregate",
    relationship_selector=none,
    nth=none,
    join_condition=join_condition,
    filters=filters,
    extra_joins=extra_joins
)) %}

{% endmacro %}
