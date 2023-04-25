{{ config(
    data_types={
        'referrer_url': dbt.type_string(),
    },
    stream='customer_stream'
)}}

with base as (
    select *
    from {{ ref('visited_page') }}
)
{{ dbt_aql.build_activity('base')}}
