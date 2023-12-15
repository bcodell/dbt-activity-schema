{% materialization dataset_column, default %}
  {%- set target_relation = api.Relation.create(identifier=model['alias'], type='cte') -%}
  {%- set aql = config.require('aql') -%}
  {% set macro_deps = model['depends_on']['macros'] -%}
  {% if 'macro.dbt_aql.dataset_column' not in macro_deps %}
    {%- set error_message -%}
      Dataset Column model '{{ model.unique_id }}' is missing required dependency on the dataset_column macro. The macro should be explicitly called in the model.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {% endif %}

  {% call noop_statement('main', model.unique_id) -%}
    {{sql}}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro dataset_column(aql) %}
{{ adapter.dispatch('dataset_column', 'dbt_aql')(aql)}}
{% endmacro %}

{% macro default__dataset_column(aql) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set query_no_comments = dbt_aql._strip_comments(aql) -%}
{%- set query_clean = dbt_aql._clean_query(query_no_comments) -%}
{%- set using, rest = dbt_aql._parse_keyword(query_clean, ["using"]) -%}
{%- set stream, rest = dbt_aql._parse_stream(rest) -%}
{%- set activity, rest = dbt_aql._parse_activity(rest, stream, [av.append, av.aggregate]) -%}

-- depends_on: {{ ref(stream) }}

{%- set model_prefix = dbt_aql.get_model_prefix(stream) -%}
{% if modules.re.search(model_prefix, activity.activity_name) is none %}
    {% set m = model_prefix~activity.activity_name %}
-- depends_on: {{ ref(m) }}
    {% else %}
-- depends_on: {{ ref(primary_activity.activity_name) }}
{% endif %}


{% endmacro %}