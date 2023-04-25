{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
aggregate between visited_page (
    count(activity_id) as pages_visited_between
)
aggregate between bought_something(
    sum(total_items_purchased) as total_items_purchased,
    sum(total_sales) as total_sales_between,
    count(activity_id) as total_purchases_between
)
{% endset %}

-- depends_on: {{ ref('output__select_first_aggregate_between') }}

{{ dbt_aql.dataset(aql) }}
