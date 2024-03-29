{% set aql %}
using customer_stream
select all signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append first ever visited_page (
    referrer_url as first_ever_referrer_url
)
append first ever bought_something(
    total_items_purchased as first_ever_total_items_purchased,
    ts as first_ever_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_all_append_first_ever') }}

{{ dbt_activity_schema.dataset(aql) }}
