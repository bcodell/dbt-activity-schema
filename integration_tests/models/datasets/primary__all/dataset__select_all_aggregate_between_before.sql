{% set aql %}
using customer_stream
select all visited_page (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as visited_page_at
)
append first between_before visited_page (
    referrer_url as first_between_before_referrer_url
)
append first between_before bought_something(
    total_items_purchased as first_between_before_total_items_purchased,
    ts as first_between_before_bought_something_at
)
{% endset %}

-- depends_on: {{ ref("output__select_all_aggregate_between_before") }}

{{ dbt_aql.dataset(aql) }}
