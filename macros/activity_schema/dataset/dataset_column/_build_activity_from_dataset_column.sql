{% macro _build_activity_from_dataset_column(stream, dataset_column) %}

{%- if execute -%}
    {%- set ws = dbt_activity_schema.whitespace() -%}
    {%- set av = dbt_activity_schema._activity_verbs() -%}
    {%- set project_name = model['unique_id'].split(".")[1] -%}
    {%- set model_prefix = dbt_activity_schema.get_model_prefix(stream) -%}
    {%- if modules.re.search(model_prefix, dataset_column) is none -%}
        {%- set dataset_column = model_prefix~dataset_column -%}
    {%- endif -%}

    {%- set activity_node = graph.get("nodes", {}).get("model."~project_name~"."~dataset_column, none) -%}
    {%- if activity_node is none -%}
        {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please choose a valid dataset column - selected '{{dataset_column}}'
Full dataset column query:

{{aql}}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    {%- set aql = activity_node.get("config", {}).get("aql", none) -%}
    {%- if aql is none -%}
        {%- set error_message -%}
    Dataset column model '{{dataset_column}}' does not have a required config key called `aql`.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {%- set query_no_comments = dbt_activity_schema._strip_comments(aql) -%}
    {%- set query_clean = dbt_activity_schema._clean_query(query_no_comments) -%}
    {%- set using, rest = dbt_activity_schema._parse_keyword(query_clean, ["using"]) -%}
    {%- set stream, rest = dbt_activity_schema._parse_stream(rest) -%}
    {%- set activity, rest = dbt_activity_schema._parse_activity(rest, stream, [av.aggregate, av.append]) -%}

    {%- if activity.columns|length != 1 -%}
        {%- set error_message -%}
Dataset column model '{{dataset_column}}' should only specify one column but it appears to specify multiple.
Full dataset column query:

{{aql}}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {%- set col = activity.columns[0] -%}
    {%- set col.alias = dataset_column.replace(model_prefix, "") -%}
    {%- set activity.columns = [col] -%}

    {%- do return(activity) -%}
{%- else -%}
    {%- do return(none) -%}        
{%- endif -%}

{% endmacro %}