{% set aql %}
using customer_stream
select nth(1) signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append first between_after visited_page (
    referrer_url as first_between_after_referrer_url
)
append first between_after bought_something(
    total_items_purchased as first_between_after_total_items_purchased,
    ts as first_between_after_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_nth_append_first_between_after') }}

{{ dbt_aql.dataset(aql) }}
