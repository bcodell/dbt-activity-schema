{% macro _base_aggregation_map() %}
{%- set valid_aggregations = [
    "count_distinct",
    "count",
    "first_value",
    "is_null",
    "last_value",
    "listagg_distinct",
    "listagg",
    "max",
    "min",
    "not_null",
    "sum_bool",
    "sum",
] -%}

{%- set zero_fill_aggregations = [
    dbt_activity_schema.aggfunc_count_distinct,
    dbt_activity_schema.aggfunc_count,
    dbt_activity_schema.aggfunc_sum,
    dbt_activity_schema.aggfunc_sum_bool,
] -%}

{%- do return(namespace(
    count_distinct=dbt_activity_schema.aggfunc_count_distinct,
    count=dbt_activity_schema.aggfunc_count,
    first_value=dbt_activity_schema.aggfunc_first_value,
    is_null=dbt_activity_schema.aggfunc_is_null,
    last_value=dbt_activity_schema.aggfunc_last_value,
    listagg_distinct=dbt_activity_schema.aggfunc_listagg_distinct,
    listagg=dbt_activity_schema.aggfunc_listagg,
    max=dbt_activity_schema.aggfunc_max,
    min=dbt_activity_schema.aggfunc_min,
    not_null=dbt_activity_schema.aggfunc_not_null,
    sum_bool=dbt_activity_schema.aggfunc_sum_bool,
    sum=dbt_activity_schema.aggfunc_sum,
    zero_fill_aggregations=zero_fill_aggregations,
    valid_aggregations=valid_aggregations
)) -%}
{% endmacro %}

{% macro _aggregation_map() %}
{%- set base_map = dbt_activity_schema._base_aggregation_map() %}
{%- do return(base_map) -%}
{% endmacro %}

{% macro zero_fill(column_name) %}
coalesce({{ column_name }}, 0)
{%- endmacro -%}
