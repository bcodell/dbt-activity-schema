{{ config(
    data_types={
        'total_sales': dbt.type_int(),
        'total_items_purchased': dbt.type_int(),
    },
    stream='customer_stream'
)}}

with base as (
    select *
    from {{ ref('bought_something') }}
)
{{ dbt_aql.build_activity('base')}}
