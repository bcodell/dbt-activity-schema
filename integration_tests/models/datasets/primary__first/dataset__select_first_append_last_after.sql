{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last after visited_page (
    referrer_url as last_after_referrer_url
)
append last after bought_something(
    total_items_purchased as last_after_total_items_purchased,
    ts as last_after_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_first_append_last_after') }}

{{ dbt_activity_schema.dataset(aql) }}
