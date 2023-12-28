{% macro default__listagg_delimiter() %}
{%- do return(dbt.string_literal(",")) -%}
{% endmacro %}
