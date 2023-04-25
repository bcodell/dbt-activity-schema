{% macro alias_activity(activity) %}
{%- set av = dbt_aql._activity_verbs() -%}
{%- if activity.verb == av.select -%}
    {%- set prefix = "primary" -%}
    {%- set attributes = [prefix, activity.relationship_selector, activity.activity_name, activity.nth] -%}
{%- else -%}
    {%- set prefix = "joined" -%}
    {%- if activity.verb == av.append -%}
        {%- set attributes = [prefix, activity.verb, activity.relationship_selector, activity.join_condition, activity.activity_name, activity.nth] -%}
    {%- elif activity.verb == av.aggregate -%}
        {%- set attributes = [prefix, activity.verb, activity.join_condition, activity.activity_name] -%}
    {%- endif -%}
{%- endif -%}

{%- do return(attributes|join("__")) -%}
{% endmacro %}

{% macro alias_column(activity_name, column_name, verb, relationship_selector=none, join_condition=none, nth=none) %}
{%- set attributes = [activity_name, verb] -%}
{%- if relationship_selector is not none -%}
    {%- do attributes.append(relationship_selector) -%}
{%- endif -%}
{%- if nth is not none -%}
    {%- do attributes.append(nth) -%}
{%- endif -%}
{%- if join_condition is not none -%}
    {%- do attributes.append(join_condition) -%}
{%- endif -%}
{%- do attributes.append(column_name) -%}
{%- do return(attributes|join("_")) -%}
{% endmacro %}

{% macro primary() %}
{%- do return("p") -%}
{% endmacro %}

{% macro joined() %}
{%- do return("j") -%}
{% endmacro %}

{% macro clean_activity_name(stream, activity_name) %}
{{ return(adapter.dispatch('clean_activity_name', 'dbt_aql')(stream, activity_name)) }}
{% endmacro %}

{% macro default__clean_activity_name(stream, activity_name) %}
{%- if execute -%}
    {%- set model_prefix = var("dbt_aql").get(stream, {}).get("model_prefix", "") -%}
{%- else -%}
    {%- set model_prefix = "__" -%}
{%- endif -%}

{%- set name_split = modules.re.split(model_prefix, activity_name) -%}
{%- if name_split|length > 1 -%}
{%- do return(dbt.string_literal(modules.re.sub("_", " ", name_split[1]))) -%}
{%- else -%}
{%- do return(dbt.string_literal(modules.re.sub("_", " ", name_split[0]))) -%}
{%- endif -%}
{% endmacro %}

{% macro _required_prefix() %}
{%- do return("req__") -%}
{% endmacro %}