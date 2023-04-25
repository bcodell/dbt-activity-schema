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
    dbt_aql.aggfunc_count_distinct,
    dbt_aql.aggfunc_count,
    dbt_aql.aggfunc_sum,
    dbt_aql.aggfunc_sum_bool,
] -%}

{%- do return(namespace(
    count_distinct=dbt_aql.aggfunc_count_distinct,
    count=dbt_aql.aggfunc_count,
    first_value=dbt_aql.aggfunc_first_value,
    is_null=dbt_aql.aggfunc_is_null,
    last_value=dbt_aql.aggfunc_last_value,
    listagg_distinct=dbt_aql.aggfunc_listagg_distinct,
    listagg=dbt_aql.aggfunc_listagg,
    max=dbt_aql.aggfunc_max,
    min=dbt_aql.aggfunc_min,
    not_null=dbt_aql.aggfunc_not_null,
    sum_bool=dbt_aql.aggfunc_sum_bool,
    sum=dbt_aql.aggfunc_sum,
    zero_fill_aggregations=zero_fill_aggregations,
    valid_aggregations=valid_aggregations
)) -%}
{% endmacro %}

{% macro _aggregation_map() %}
{%- set base_map = dbt_aql._base_aggregation_map() %}
{%- do return(base_map) -%}
{% endmacro %}

{% macro zero_fill(column_name) %}
coalesce({{ column_name }}, 0)
{%- endmacro -%}
