-- depends_on: {{ ref('output__filtered__query_stream_macro') }}


{{ dbt_aql.query_stream(
    stream='customer_stream',
    primary_activity=dbt_aql.primary_activity(
        activity='customer__visited_page',
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
        dbt_aql.appended_activity(
            activity='customer__visited_page',
            relationship_selector='first',
            join_condition='ever',
            columns=[
                dbt_aql.dc(column_name='ts', alias='first_visited_yahoo_at')
            ],
            filters=[
                dbt_aql.json_extract('{feature_json}', 'referrer_url')~" = 'yahoo.com'"
            ]
        ),
        dbt_aql.aggregated_activity(
            activity='customer__bought_something',
            join_condition='all',
            columns=[
                dbt_aql.dc(column_name='activity_id', alias='total_large_purchases', aggfunc='count'),
            ],
            filters=[
                'cast(nullif('~dbt_aql.json_extract('{joined}.{feature_json}', 'total_sales')~", '') as int) > 100",
                'cast(nullif('~dbt_aql.json_extract('{joined}.{feature_json}', 'total_items_purchased')~", '') as int) > 3"
            ]
        )
    ],
    included_columns=[
        'customer__total_items_purchased_after'
    ]
) }}