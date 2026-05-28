{{
    config(
        enabled=target.name == 'redshift',
        tags=['redshift']
    )
}}

-- Tests that build_activity (fixed redshift__build_json) correctly stores and
-- retrieves text fields containing quotes/backslashes and boolean fields.
-- Run on Redshift: dbt build --select tag:redshift

-- depends_on: {{ ref('output__redshift_edge_cases') }}

select
    CAST({{ dbt_activity_schema.schema_columns('customer_stream').customer }} AS varchar) as customer_id,
    CAST(attributes.text_with_quotes AS varchar) as text_with_quotes,
    CAST(attributes.text_with_backslash AS varchar) as text_with_backslash,
    CAST(attributes.is_flagged AS varchar) as is_flagged
from {{ ref('customer__redshift_edge_cases') }}
order by customer_id
