{% set aql %}
using customer_stream
select first visited_page (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as first_visited_google_at
    filter {{dbt_activity_schema.json_extract('{feature_json}', 'referrer_url')}} = 'google.com'
)
aggregate after bought_something (
    count(activity_id) as total_large_purchases_after
    join cast(nullif({{dbt_activity_schema.json_extract('{joined}.{feature_json}', 'total_sales')}}, '') as int) > 100
)
include (
    total_items_purchased_after
)
{% endset %}

-- depends_on: {{ ref('output__joined') }}

{{ dbt_activity_schema.dataset(aql) }}
