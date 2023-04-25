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
{%- set req = dbt_aql._required_prefix() -%}
{{dbt_aql.primary()}}.{{req}}{{ts}} < {{dbt_aql.joined()}}.{{ts}}
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
    all=dbt_aql._join_clause_all,
    ever=dbt_aql._join_clause_ever
)) -%}
{% endmacro %}