{% macro _relationship_clause_first(verb, join_condition, nth) %}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set jc = dbt_activity_schema._join_conditions() -%}
{%- set activity_occurrence = dbt_activity_schema.schema_columns().activity_occurrence -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_activity_schema.primary() -%}
    {%- set statement -%}
{{alias}}.{{activity_occurrence}} = 1
{%- endset -%}
{%- elif verb == av.append -%}
    {%- set alias = dbt_activity_schema.joined() -%}
    {%- if join_condition in [jc.ever, jc.before] -%}
        {%- set statement -%}
{{alias}}.{{activity_occurrence}} = 1
        {%- endset -%}
    {%- elif join_condition in [jc.after, jc.between] -%}
        {%- set statement = none -%}
    {%- endif -%}
{%- endif -%}
{%- do return(statement) -%}
{% endmacro %}

{% macro _relationship_clause_last(verb, join_condition, nth) %}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set jc = dbt_activity_schema._join_conditions() -%}
{%- set activity_repeated_at = dbt_activity_schema.schema_columns().activity_repeated_at -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_activity_schema.primary() -%}
    {%- set statement -%}
{{alias}}.{{activity_repeated_at}} is null
{%- endset -%}
{%- elif verb == av.append -%}
    {%- set alias = dbt_activity_schema.joined() -%}
    {%- if join_condition in [jc.ever, jc.after] -%}
        {%- set statement -%}
{{alias}}.{{activity_repeated_at}} is null
        {%- endset -%}
    {%- elif join_condition in [jc.before, jc.between] -%}
        {%- set statement = none -%}
    {%- endif -%}
{%- endif -%}
{%- do return(statement) -%}
{% endmacro %}

{% macro _relationship_clause_all(verb, join_condition, nth) -%}
true
{%- endmacro -%}


{% macro _relationship_clause_nth(verb, join_condition, nth) %}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set activity_occurrence = dbt_activity_schema.schema_columns().activity_occurrence -%}
{%- if verb == av.select -%}
    {%- set alias = dbt_activity_schema.primary() -%}
{%- else -%}
    {%- set alias = dbt_activity_schema.joined() -%}
{%- endif -%}
{{alias}}.{{activity_occurrence}} = {{nth}}
{% endmacro %}


{% macro _relationship_clause_map() %}
{%- do return(namespace(
    first=dbt_activity_schema._relationship_clause_first,
    last=dbt_activity_schema._relationship_clause_last,
    all=dbt_activity_schema._relationship_clause_all,
    nth=dbt_activity_schema._relationship_clause_nth
)) -%}
{% endmacro %}
