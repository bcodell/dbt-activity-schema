-- depends_on: {{ ref('output__joined__query_stream_macro') }}


{{ dbt_activity_schema.query_stream(
    stream='customer_stream',
    primary_activity=dbt_activity_schema.primary_activity(
        activity='visited_page',
        relationship_selector='first',
        columns=[
            dbt_activity_schema.dc(column_name='activity_id', alias='activity_id'),
            dbt_activity_schema.dc(column_name='entity_uuid', alias='customer_id'),
            dbt_activity_schema.dc(column_name='ts', alias='first_visited_google_at'),
        ],
        filters=[
            dbt_activity_schema.json_extract('{feature_json}', 'referrer_url')~" = 'google.com'"
        ]
    ),
    joined_activities=[
        dbt_activity_schema.aggregated_activity(
            activity='bought_something',
            join_condition='after',
            columns=[
                dbt_activity_schema.dc(column_name='activity_id', alias='total_large_purchases_after', aggfunc='count')
            ],
            extra_joins=[
                'cast(nullif('~dbt_activity_schema.json_extract('{joined}.{feature_json}', 'total_sales')~", '') as int) > 100"
            ]
        )
    ],
    included_columns=[
        'total_items_purchased_after'
   ]
) }}