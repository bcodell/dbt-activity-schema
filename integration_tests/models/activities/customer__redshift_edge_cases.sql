{{
    config(
        enabled=target.name == 'redshift',
        tags=['redshift'],
        stream='customer_stream',
        data_types={
            'text_with_quotes': dbt.type_string(),
            'text_with_backslash': dbt.type_string(),
            'is_flagged': dbt_activity_schema.type_boolean(),
        }
    )
}}

with base as (
    select *
    from {{ ref('redshift_edge_cases_source') }}
)
{{ dbt_activity_schema.build_activity('base', null_columns=['anonymous_customer_id', 'revenue_impact', 'link']) }}
