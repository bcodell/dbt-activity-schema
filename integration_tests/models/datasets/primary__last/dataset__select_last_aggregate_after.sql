{% set aql %}
using customer_stream
select last signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
aggregate after visited_page (
    count(activity_id) as pages_visited_after
)
aggregate after bought_something(
    sum(total_items_purchased) as total_items_purchased_after,
    sum(total_sales) as total_sales_after,
    count(activity_id) as total_purchases_after
)
{% endset %}

-- depends_on: {{ ref('output__select_last_aggregate_after') }}

{{ dbt_aql.dataset(aql) }}
