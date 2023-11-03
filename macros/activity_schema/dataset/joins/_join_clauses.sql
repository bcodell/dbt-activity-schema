{% macro _join_clause_after() %}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} < {{dbt_aql.joined()}}.{{ts}}
{%- endmacro %}

{% macro _join_clause_before() %}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} > {{dbt_aql.joined()}}.{{ts}}
{%- endmacro %}

{% macro _join_clause_between() %}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set next_ts = dbt_aql.schema_columns().activity_repeated_at -%}
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} < {{dbt_aql.joined()}}.{{ts}} and ({{dbt_aql.primary()}}.{{req}}{{next_ts}} > {{dbt_aql.joined()}}.{{ts}} or {{dbt_aql.primary()}}.{{req}}{{next_ts}} is null)
{%- endmacro %}

{% macro _join_clause_between_after() %}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set next_ts = dbt_aql.schema_columns().activity_repeated_at -%}
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} < {{dbt_aql.joined()}}.{{ts}} and ({{dbt_aql.primary()}}.{{req}}{{next_ts}} > {{dbt_aql.joined()}}.{{ts}} or {{dbt_aql.primary()}}.{{req}}{{next_ts}} is null)
{%- endmacro %}

{% macro _join_clause_between_before() %}
{%- set ts = dbt_aql.schema_columns().ts -%}
{%- set previous_ts = dbt_aql.schema_columns().previous_activity_occurrence_at -%}
{%- set activity_occurrence = dbt_aql.schema_columns().activity_occurrence -%}
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} > {{dbt_aql.joined()}}.{{ts}} and ({{dbt_aql.primary()}}.{{req}}{{previous_ts}} < {{dbt_aql.joined()}}.{{ts}} or {{dbt_aql.primary()}}.{{req}}{{activity_occurrence}} = 1)
{%- endmacro %}

{% macro _join_clause_all() %}
true
{%- endmacro %}

{% macro _join_clause_ever() %}
true
{%- endmacro %}

{% macro _join_clause_map() %}
{%- do return(namespace(
    after=dbt_aql._join_clause_after,
    before=dbt_aql._join_clause_before,
    between=dbt_aql._join_clause_between,
    between_after=dbt_aql._join_clause_between_after,
    between_before=dbt_aql._join_clause_between_before,
    all=dbt_aql._join_clause_all,
    ever=dbt_aql._join_clause_ever
)) -%}
{% endmacro %}