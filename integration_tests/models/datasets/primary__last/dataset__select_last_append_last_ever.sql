{% set aql %}
using customer_stream
select last signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last ever visited_page (
    referrer_url as last_ever_referrer_url
)
append last ever bought_something(
    total_items_purchased as last_ever_total_items_purchased,
    ts as last_ever_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_last_append_last_ever') }}

{{ dbt_activity_schema.dataset(aql) }}
