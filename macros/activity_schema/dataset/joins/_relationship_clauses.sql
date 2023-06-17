{% macro _relationship_clause_first(verb, join_condition, nth) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set jc = dbt_aql._join_conditions() -%}
{%- set activity_occurrence = dbt_aql.schema_columns().activity_occurrence -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_aql.primary() -%}
    {%- set statement -%}
{{alias}}.{{activity_occurrence}} = 1
{%- endset -%}
{%- elif verb == av.append -%}
    {%- set alias = dbt_aql.joined() -%}
    {%- if join_condition in [jc.ever, jc.before] -%}
        {%- set statement -%}
{{alias}}.{{activity_occurrence}} = 1
        {%- endset -%}
    {%- elif join_condition in [jc.after, jc.between, jc.between_before, jc.between_after] -%}
        {%- set statement = none -%}
    {%- endif -%}
{%- endif -%}
{%- do return(statement) -%}
{% endmacro %}

{% macro _relationship_clause_last(verb, join_condition, nth) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set jc = dbt_aql._join_conditions() -%}
{%- set activity_repeated_at = dbt_aql.schema_columns().activity_repeated_at -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_aql.primary() -%}
    {%- set statement -%}
{{alias}}.{{activity_repeated_at}} is null
{%- endset -%}
{%- elif verb == av.append -%}
    {%- set alias = dbt_aql.joined() -%}
    {%- if join_condition in [jc.ever, jc.after] -%}
        {%- set statement -%}
{{alias}}.{{activity_repeated_at}} is null
        {%- endset -%}
    {%- elif join_condition in [jc.before, jc.between, jc.between_before, jc.between_after] -%}
        {%- set statement = none -%}
    {%- endif -%}
{%- endif -%}
{%- do return(statement) -%}
{% endmacro %}

{% macro _relationship_clause_all(verb, join_condition, nth) -%}
true
{%- endmacro -%}


{% macro _relationship_clause_nth(verb, join_condition, nth) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set activity_occurrence = dbt_aql.schema_columns().activity_occurrence -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_aql.primary() -%}
{%- else -%}
    {%- set alias = dbt_aql.joined() -%}
{%- endif -%}
{{alias}}.{{activity_occurrence}} = {{nth}}
{% endmacro %}


{% macro _relationship_clause_map() %}
{%- do return(namespace(
    first=dbt_aql._relationship_clause_first,
    last=dbt_aql._relationship_clause_last,
    all=dbt_aql._relationship_clause_all,
    nth=dbt_aql._relationship_clause_nth
)) -%}
{% endmacro %}
