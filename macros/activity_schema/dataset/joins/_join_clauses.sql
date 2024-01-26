{% macro _join_clause_after() %}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set req = dbt_activity_schema._required_prefix() -%}
{{dbt_activity_schema.primary()}}.{{req}}{{ts}} < {{dbt_activity_schema.joined()}}.{{ts}}
{%- endmacro %}

{% macro _join_clause_before() %}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set req = dbt_activity_schema._required_prefix() -%}
{{dbt_activity_schema.primary()}}.{{req}}{{ts}} > {{dbt_activity_schema.joined()}}.{{ts}}
{%- endmacro %}

{% macro _join_clause_between() %}
{%- set ts = dbt_activity_schema.schema_columns().ts -%}
{%- set next_ts = dbt_activity_schema.schema_columns().activity_repeated_at -%}
{%- set req = dbt_activity_schema._required_prefix() -%}
{{dbt_activity_schema.primary()}}.{{req}}{{ts}} < {{dbt_activity_schema.joined()}}.{{ts}} and ({{dbt_activity_schema.primary()}}.{{req}}{{next_ts}} > {{dbt_activity_schema.joined()}}.{{ts}} or {{dbt_activity_schema.primary()}}.{{req}}{{next_ts}} is null)
{%- endmacro %}

{% macro _join_clause_all() %}
true
{%- endmacro %}

{% macro _join_clause_ever() %}
true
{%- endmacro %}

{% macro _join_clause_map() %}
{%- do return(namespace(
    after=dbt_activity_schema._join_clause_after,
    before=dbt_activity_schema._join_clause_before,
    between=dbt_activity_schema._join_clause_between,
    all=dbt_activity_schema._join_clause_all,
    ever=dbt_activity_schema._join_clause_ever
)) -%}
{% endmacro %}