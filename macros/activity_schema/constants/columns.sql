{% macro customer_column(stream) %}
{%- set customer_alias = var("dbt_aql").get("streams", {}).get(stream, {}).get("customer_id_alias", none) -%}
{%- if customer_alias is none -%}
    {%- set error_message -%}
Configuration for stream '{{stream}}' does not include required alias for customer ID column.
Please add `customer_id_alias` in your dbt_project.yml configuration for stream '{{stream}}'
    {%- endset -%}
    {{ exceptions.warn(error_message) }}
{%- else -%}
    {%- do return(customer_alias) -%}
{%- endif -%}
{% endmacro %}


{% macro anonymous_customer_column(stream) %}
{%- do return(var("dbt_aql").get("streams").get(stream, {}).get("anonymous_customer_id_alias", none)) -%}
{% endmacro %}


{% macro schema_columns(stream=none) %}
{# returns a dict where each key is the standard activity schema column name and each value is the alias configured #}
{%- set required_columns = dict(
    activity_id = "activity_id",
    ts = "ts",
    customer = "customer",
    activity = "activity",
    feature_json = "feature_json",
    activity_occurrence = "activity_occurrence",
    activity_repeated_at = "activity_repeated_at",
) -%}

{%- set optional_columns = dict(
    revenue_impact = "revenue_impact",
    link = "link"
) -%}

{%- set column_config = var("dbt_aql").get("column_configuration", {}) -%}
{%- set included_optional_columns = column_config.get("included_optional_columns", []) -%}
{#
check for adherence to activity schema spec naming conventions
#}
{%- for col in included_optional_columns -%}
    {%- if col in optional_columns.keys() -%}
        {%- do required_columns.update({col: optional_columns.get(col)}) -%}
    {%- else -%}
        {%- set error_message -%}
Activity Schema column configuration specifies invalid optional column '{{col}}'. Valid optional columns
to pick are '{{optional_columns.keys()}}'
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{%- endfor -%}
{#
check for adherence to activity schema spec naming conventions
#}
{%- set stream_aliases = column_config.get("column_aliases", {}) -%}
{%- for col in stream_aliases.keys() -%}
    {%- if col not in required_columns.keys() -%}
        {%- set error_message -%}
Activity Schema column configuration is attempting to alias invalid column '{{col}}'. Valid columns
to alias are '{{required_columns.keys()}}'
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{%- endfor -%}

{%- do required_columns.update(stream_aliases) -%}

{%- if stream is not none -%}
    {%- do required_columns.update({"customer": dbt_aql.customer_column(stream)}) -%}
    {%- if dbt_aql.anonymous_customer_column(stream) is not none -%}
        {%- do required_columns.update({"anonymous_customer_id": dbt_aql.anonymous_customer_column(stream)}) -%}
    {%- endif -%}
{%- endif -%}
{%- do return(required_columns) -%}

{% endmacro %}


{% macro schema_column_types(stream=none) %}
{# returns a dict where each key is the alias of the activity schema column and each value is the corresponding data type #}
{%- set columns = dbt_aql.schema_columns(stream) -%}
{%- set column_types = {
    columns.activity_id: dbt.type_string(),
    columns.ts: dbt.type_timestamp(),
    columns.activity: dbt.type_string(),
    columns.feature_json: dbt_aql.type_json(),
    columns.activity_occurrence: dbt.type_int(),
    columns.activity_repeated_at: dbt.type_timestamp()
} -%}
{%- if "revenue_impact" in columns.keys() -%}
    {%- do column_types.update({columns.revenue_impact: dbt.type_int()}) -%}
{%- endif -%}
{%- if "link" in columns.keys()  -%}
    {%- do column_types.update({columns.link: dbt.type_string()}) -%}
{%- endif -%}
{%- if stream is not none -%}
    {%- set customer_col = dbt_aql.customer_column(stream) -%}
    {%- do column_types.update({customer_col: dbt.type_string()}) -%}
    {%- if columns.anonymous_customer_id is defined -%}
        {%- do column_types.update({columns.anonymous_customer_id: dbt.type_string()}) -%}
    {%- endif -%}
{%- else -%}
    {%- do column_types.update({"customer": dbt.type_string()}) -%}
    {%- do column_types.update({"anonymous_customer_id": dbt.type_string()}) -%}
{%- endif -%}
{%- do return(column_types) -%}
{% endmacro %}
