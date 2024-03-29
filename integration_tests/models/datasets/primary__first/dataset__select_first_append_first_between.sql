{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append first between visited_page (
    referrer_url as first_between_referrer_url
)
append first between bought_something(
    total_items_purchased as first_between_total_items_purchased,
    ts as first_between_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_first_append_first_between') }}

{{ dbt_activity_schema.dataset(aql) }}
