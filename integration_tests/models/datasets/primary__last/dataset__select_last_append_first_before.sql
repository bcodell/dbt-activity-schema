{% set aql %}
using customer_stream
select last signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append first before visited_page (
    referrer_url as first_before_referrer_url
)
append first before bought_something(
    total_items_purchased as first_before_total_items_purchased,
    ts as first_before_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_last_append_first_before') }}

{{ dbt_aql.dataset(aql) }}
