-- depends_on: {{ ref('output__joined__query_stream_macro') }}


{{ dbt_aql.query_stream(
    stream='customer_stream',
    primary_activity=dbt_aql.primary_activity(
        activity='visited_page',
        relationship_selector='first',
        columns=[
            dbt_aql.dc(column_name='activity_id', alias='activity_id'),
            dbt_aql.dc(column_name='entity_uuid', alias='customer_id'),
            dbt_aql.dc(column_name='ts', alias='first_visited_google_at'),
        ],
        filters=[
            dbt_aql.json_extract('{feature_json}', 'referrer_url')~" = 'google.com'"
        ]
    ),
    joined_activities=[
        dbt_aql.aggregated_activity(
            activity='bought_something',
            join_condition='after',
            columns=[
                dbt_aql.dc(column_name='activity_id', alias='total_large_purchases_after', aggfunc='count')
            ],
            extra_joins=[
                'cast(nullif('~dbt_aql.json_extract('{joined}.{feature_json}', 'total_sales')~", '') as int) > 100"
            ]
        )
    ],
    included_columns=[
        'total_items_purchased_after'
   ]
) }}