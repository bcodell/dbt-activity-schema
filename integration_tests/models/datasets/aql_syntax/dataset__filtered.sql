{% set aql %}
using customer_stream
select first visited_page (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as first_visited_google_at
    filter {{dbt_aql.json_extract('{feature_json}', 'referrer_url')}} = 'google.com'
)
append first ever visited_page (
    ts as first_visited_yahoo_at
    filter {{dbt_aql.json_extract('{feature_json}', 'referrer_url')}} = 'yahoo.com'
)
aggregate all bought_something (
    count(activity_id) as total_large_purchases_after
    filter cast(nullif({{dbt_aql.json_extract('{feature_json}', 'total_sales')}}, '') as int) > 100
    filter cast(nullif({{dbt_aql.json_extract('{feature_json}', 'total_items_purchased')}}, '') as int) > 3
)
include (
    total_items_purchased_after
)
{% endset %}

-- depends_on: {{ ref('output__filtered') }}

{{ dbt_aql.dataset(aql) }}
