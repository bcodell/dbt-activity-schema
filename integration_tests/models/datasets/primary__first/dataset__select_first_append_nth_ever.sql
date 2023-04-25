{% set aql %}
using customer_stream
select first signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append nth(2) ever visited_page (
    referrer_url as first_ever_referrer_url,
    ts as second_pageview_at
)
{% endset %}

-- depends_on: {{ ref('output__select_first_append_nth_ever') }}

{{ dbt_aql.dataset(aql) }}
