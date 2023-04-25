{% macro whitespace() %}
{%- do return("\s+") -%}
{% endmacro %}

{% macro punctuation() %}
{%- set single_quote = "'" -%}
{%- set double_quote = '"' -%}
{%- do return("!#$%&()+,-./:;<=>?@[\\]^`{|}~"~single_quote~double_quote) -%}
{% endmacro %}

{% macro select_whitespace() %}
{%- do return('[{whitespace}]select[{whitespace}]'.format(whitespace=dbt_aql.whitespace())) -%}
{% endmacro %}
