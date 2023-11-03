{% set aql %}
using customer_stream
select all signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last between_before visited_page (
    referrer_url as last_between_before_referrer_url
)
append last between_before bought_something(
    total_items_purchased as last_between_before_total_items_purchased,
    ts as last_between_before_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_all_append_last_between_before') }}

{{ dbt_aql.dataset(aql) }}
