{% set aql %}
using customer_stream
select last signed_up (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as signed_up_at
)
append nth(2) before visited_page (
    referrer_url as first_before_referrer_url,
    ts as second_pageview_at
)
{% endset %}

-- depends_on: {{ ref('output__select_last_append_nth_before') }}

{{ dbt_aql.dataset(aql) }}
