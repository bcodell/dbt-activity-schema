{% set aql %}
using customer_stream
select all visited_page (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as visited_page_at
)
aggregate between_before visited_page (
    first_value(referrer_url) as first_between_before_referrer_url
)
aggregate between_before bought_something(
    sum(total_items_purchased) as total_items_purchased_between_before,
    last_value(previous_activity_occurred_at) as penultimate_bought_something_at_between_before
)
{% endset %}

-- depends_on: {{ ref("output__select_all_aggregate_between_before") }}

{{ dbt_aql.dataset(aql) }}
