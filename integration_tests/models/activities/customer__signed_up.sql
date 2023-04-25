{{ config(stream='customer_stream') }}

with base as (
    select *
    from {{ ref('signed_up') }}
)
{{ dbt_aql.build_activity('base')}}
