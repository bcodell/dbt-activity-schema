{% set aql %}
using customer_stream
select nth(1) signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last between_after visited_page (
    referrer_url as last_between_after_referrer_url
)
append last between_after bought_something(
    total_items_purchased as last_between_after_total_items_purchased,
    ts as last_between_after_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_nth_append_last_between_after') }}

{{ dbt_aql.dataset(aql) }}
