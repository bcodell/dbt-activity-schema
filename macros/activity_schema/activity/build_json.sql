{%- macro build_json(data_types) -%}
    {{ return(adapter.dispatch('build_json', 'dbt_activity_schema')(data_types)) }}
{%- endmacro -%}


{%- macro default__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    json_build_object(
        {% for feature in features -%}
        '{{feature}}', {{feature}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro postgres__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    json_build_object(
        {% for feature in features -%}
        '{{feature}}', {{feature}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro redshift__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    json_parse('{' ||
        {% for feature in features -%}
        {%- set dtype = data_types[feature] | lower -%}
        {% if not loop.first -%}', '|| {%- endif -%}'"{{feature}}": "' ||
        {%- if 'bool' in dtype -%}
        case when {{feature}} is true then 'true' when {{feature}} is false then 'false' else '' end
        {%- else -%}
        replace(replace(decode(cast({{feature}} as {{dbt.type_string()}}), null, '', cast({{feature}} as {{dbt.type_string()}})), '\\', '\\\\'), '"', '\\"')
        {%- endif -%}
        {% if not loop.last %} ||'"'{% endif %}
        {% endfor -%}
    || '"}')
    {%- else -%}
    cast(null as super)
    {%- endif -%}
{%- endmacro -%}



{%- macro bigquery__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    to_json(struct(
        {% for feature in features -%}
        {{feature}} as {{feature}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    ))
    {%- else -%}
    to_json(null)
    {%- endif -%}
{%- endmacro -%}


{%- macro snowflake__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    object_construct(
        {% for feature in features -%}
        '{{feature}}', {{feature}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    null::object
    {%- endif -%}
{%- endmacro -%}


{%- macro duckdb__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    json_object(
        {% for feature in features -%}
        '{{feature}}', {{feature}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    null::json
    {%- endif -%}
{%- endmacro -%}


{%- macro spark__build_json(data_types) -%}
    {%- if data_types is not none -%}
    {%- set features = data_types.keys() -%}
    to_json(struct(
        {% for feature in features -%}
        {{feature}} as {{feature}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    ))
    {%- else -%}
    cast(null as string)
    {%- endif -%}
{%- endmacro -%}
