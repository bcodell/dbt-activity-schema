{% set aql %}
using customer_stream
select time_spine(interval=month, end_period=max) visited_page (
    entity_uuid as customer_id
)
aggregate between bought_something(
    sum(total_items_purchased) as total_items_purchased_between,
    sum(total_sales) as total_sales_between,
    count(activity_id) as total_purchases_between
)
{% endset %}

-- depends_on: {{ ref('output__time_spine') }}

{{ dbt_activity_schema.dataset(aql) }}
order by 3,1