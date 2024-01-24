{% set aql %}
using customer_stream
select all signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
aggregate after visited_page (
    count(activity_id) as pages_visited_after
)
aggregate after bought_something(
    sum(total_sales) as total_sales_after,
    count(activity_id) as total_purchases_after
)
include (
    total_items_purchased_after
)
{% endset %}

-- depends_on: {{ ref('output__select_all_aggregate_after') }}

{{ dbt_activity_schema.dataset(aql) }}
