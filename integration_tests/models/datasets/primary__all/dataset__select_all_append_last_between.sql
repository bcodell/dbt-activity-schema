{% set aql %}
using customer_stream
select all signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append last between visited_page (
    referrer_url as last_between_referrer_url
)
append last between bought_something(
    total_items_purchased as last_between_total_items_purchased,
    ts as last_between_bought_something_at
)
{% endset %}

-- depends_on: {{ ref('output__select_all_append_last_between') }}

{{ dbt_aql.dataset(aql) }}
