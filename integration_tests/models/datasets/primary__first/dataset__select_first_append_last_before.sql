{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last before visited_page (
    referrer_url as last_before_referrer_url
)
append last before bought_something(
    total_items_purchased as last_before_total_items_purchased,
    ts as last_before_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_first_append_last_before') }}

{{ dbt_activity_schema.dataset(aql) }}
