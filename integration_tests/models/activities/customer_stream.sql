{% set keys = dbt_activity_schema.cluster_keys() %}
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
    options={"partition_by": cluster_keys},
    cluster_by=cluster_keys,
    partition_by=partition_keys
) }}

{% set activity_list = [
    ref('customer__bought_something'),
    ref('customer__signed_up'),
    ref('customer__visited_page'),
] %}

{{ dbt_activity_schema.build_stream(activity_list) }}