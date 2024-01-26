{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append first after visited_page (
    referrer_url as first_after_referrer_url
)
append first after bought_something(
    total_items_purchased as first_after_total_items_purchased,
    ts as first_after_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_first_append_first_after') }}

{{ dbt_activity_schema.dataset(aql) }}
