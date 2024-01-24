{% set aql %}
using customer_stream
aggregate after bought_something (
    sum(total_items_purchased)
)
{% endset %}

{{config(materialized='dataset_column', aql=aql)}}

{{ dbt_activity_schema.dataset_column(aql) }}
