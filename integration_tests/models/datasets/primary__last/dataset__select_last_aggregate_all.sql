{% set aql %}
using customer_stream
select last signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
aggregate all visited_page (
    count(activity_id) as pages_visited_all
)
aggregate all bought_something(
    sum(total_items_purchased) as total_items_purchased_all,
    sum(total_sales) as total_sales_all,
    count(activity_id) as total_purchases_all
)
{% endset %}

-- depends_on: {{ ref('output__select_last_aggregate_all') }}

{{ dbt_aql.dataset(aql) }}
