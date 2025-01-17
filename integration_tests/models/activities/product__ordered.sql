{% set keys = dbt_aql.cluster_keys(stream='product_stream') %}
{% if target.name == 'bigquery' %}
    {% set cluster_keys = keys.cluster_by %}
    {% set partition_keys = keys.partition_by %}
{# TODO: remove snowflake-specific implementation once localstack bugs are fixed #}
{% elif target.name == 'snowflake' %}
    {% set cluster_keys = none %}
    {% set partition_keys = none %}
{% else %}
    {% set cluster_keys = keys %}
    {% set partition_keys = '' %}
{% endif %}

{{ config(
    cluster_by=cluster_keys,
    partition_by=partition_keys,
    stream='product_stream'
) }}

with base as (
    select *
    from {{ ref('product_ordered') }}
)
{{ dbt_aql.build_activity('base', null_columns=['revenue_impact', 'link'])}}
