{% set aql %}
using customer_stream
aggregate after bought_something (
    sum(total_items_purchased)
)
{% endset %}

{{config(materialized='dataset_column', aql=aql)}}

{{ dbt_aql.dataset_column(aql) }}
