{{ config(options=dbt_aql.cluster_keys()) }}

{% set activity_list = [
    ref('customer__bought_something'),
    ref('customer__signed_up'),
    ref('customer__visited_page'),
] %}

{{ dbt_aql.build_stream(activity_list) }}