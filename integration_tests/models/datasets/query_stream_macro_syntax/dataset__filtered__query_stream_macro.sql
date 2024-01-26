-- depends_on: {{ ref('output__filtered__query_stream_macro') }}


{{ dbt_activity_schema.query_stream(
    stream='customer_stream',
    primary_activity=dbt_activity_schema.primary_activity(
        activity='customer__visited_page',
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
        dbt_activity_schema.appended_activity(
            activity='customer__visited_page',
            relationship_selector='first',
            join_condition='ever',
            columns=[
                dbt_activity_schema.dc(column_name='ts', alias='first_visited_yahoo_at')
            ],
            filters=[
                dbt_activity_schema.json_extract('{feature_json}', 'referrer_url')~" = 'yahoo.com'"
            ]
        ),
        dbt_activity_schema.aggregated_activity(
            activity='customer__bought_something',
            join_condition='all',
            columns=[
                dbt_activity_schema.dc(column_name='activity_id', alias='total_large_purchases', aggfunc='count'),
            ],
            filters=[
                'cast(nullif('~dbt_activity_schema.json_extract('{joined}.{feature_json}', 'total_sales')~", '') as int) > 100",
                'cast(nullif('~dbt_activity_schema.json_extract('{joined}.{feature_json}', 'total_items_purchased')~", '') as int) > 3"
            ]
        )
    ],
    included_columns=[
        'customer__total_items_purchased_after'
    ]
) }}