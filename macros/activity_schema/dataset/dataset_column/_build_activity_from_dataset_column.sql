{% macro _build_activity_from_dataset_column(stream, dataset_column) %}

{%- if execute -%}
    {%- set ws = dbt_aql.whitespace() -%}
    {%- set av = dbt_aql._activity_verbs() -%}
    {%- set project_name = model['unique_id'].split(".")[1] -%}
    {%- set model_prefix = dbt_aql.get_model_prefix(stream) -%}
    {%- if modules.re.search(model_prefix, dataset_column) is none -%}
        {%- set dataset_column = model_prefix~dataset_column -%}
    {%- endif -%}

    {%- set activity_node = graph.get("nodes", {}).get("model."~project_name~"."~dataset_column, {}) -%}
    {%- set aql = activity_node.config.get("aql", none) -%}
    {%- if aql is none -%}
        {%- set error_message -%}
    Dataset column model '{{dataset_column}}' does not have a required config key called `aql`.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- else -%}

        {%- set query_no_comments = dbt_aql._strip_comments(aql) -%}
        {%- set query_clean = dbt_aql._clean_query(query_no_comments) -%}
        {%- set using, rest = dbt_aql._parse_keyword(query_clean, ["using"]) -%}
        {%- set stream, rest = dbt_aql._parse_stream(rest) -%}
        {%- set activity, rest = dbt_aql._parse_activity(rest, stream, [av.aggregate, av.append]) -%}

        {%- if activity.columns|length != 1 -%}
            {%- set error_message -%}
        Dataset column model '{{dataset_column}}' should only specify one column but it appears to specify multiple.
        Full query:

        {{aql}}
            {%- endset -%}
            {{ exceptions.raise_compiler_error(error_message) }}
        {%- else -%}
            {%- set col = activity.columns[0] -%}
            {%- set col.alias = dataset_column.replace(model_prefix, "") -%}
            {%- set activity.columns = [col] -%}
        {%- endif -%}
        {%- do return(activity) -%}
    {%- endif -%}
{%- else -%}
    {%- do return(none) -%}        
{%- endif -%}

{% endmacro %}