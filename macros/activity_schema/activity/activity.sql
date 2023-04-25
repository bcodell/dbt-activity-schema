{% macro activity(
    activity_name,
    stream,
    verb,
    join_condition,
    relationship_selector,
    columns,
    nth=none,
    extra_joins=none
) %}

{%- set join_clauses = dbt_aql._join_clause_map() -%}
{%- set relationship_clauses = dbt_aql._relationship_clause_map() -%}
{%- if join_condition is not none -%}
    {%- set join_clause = join_clauses[join_condition]() -%}
{%- else -%}
    {%- set join_clause = none -%}
{%- endif -%}
{%- if relationship_selector is not none -%}
    {%- set relationship_clause = relationship_clauses[relationship_selector](verb, join_condition, nth) -%}
    {%- if relationship_clause is not defined -%}
        {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Parsed invalid relationship selector
'{{relationship_selector}}' for activity '{{verb}} {{activity_name}}'.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{%- else -%}
    {%- set relationship_clause = none -%}
{%- endif -%}

{%- do return(namespace(
    name="activity",
    verb=verb,
    relationship_selector=relationship_selector,
    join_condition=join_condition,
    join_clause=join_clause,
    relationship_clause=relationship_clause,
    activity_name=activity_name,
    columns=columns,
    nth=nth,
    extra_joins=extra_joins
)) -%}


{% endmacro %}