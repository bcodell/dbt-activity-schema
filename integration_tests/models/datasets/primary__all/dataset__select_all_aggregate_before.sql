{% set aql %}
using customer_stream
select all signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
aggregate before visited_page (
    count(activity_id) as pages_visited_before
)
aggregate before bought_something(
    sum(total_items_purchased) as total_items_purchased,
    sum(total_sales) as total_sales_before,
    count(activity_id) as total_purchases_before
)
{% endset %}

-- depends_on: {{ ref('output__select_all_aggregate_before') }}

{{ dbt_aql.dataset(aql) }}
