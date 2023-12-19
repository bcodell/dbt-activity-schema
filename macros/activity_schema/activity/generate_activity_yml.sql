-- Get data_types object from model config block
{% macro get_activity_config(model) -%}
    {% set relation = ref(model) %}
    {% for node in graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", relation.identifier) %}
        {% do return(node.config) %}
    {% endfor %}
{%- endmacro %}

{% macro generate_activity_yml(activities) %}

{% set yaml_output = [] %}

{% do yaml_output.append('version: 2') %}
{% do yaml_output.append('models:') %}

-- Loop through each activity
{% for activity in activities %}

  {% do yaml_output.append('  - name: ' ~ activity) %}
  {% do yaml_output.append('    columns:') %}

{% set columns = [
    {'name': 'activity_id', 'description': 'Unique identifier for the activity.', 'data_type': type_string(), 'tests': ['unique', 'not_null']},
    {'name': 'customer_id', 'description': 'Identifier for the customer.', 'data_type': type_string()},
    {'name': 'activity', 'description': 'Type of activity performed by the customer.', 'data_type': type_string(), 'tests': ['not_null']},
    {'name': 'ts', 'description': 'Timestamp of when the activity occurred.', 'data_type': type_timestamp(), 'tests': ['not_null']},
    {'name': 'revenue_impact', 'description': 'Revenue impact of the activity, if applicable.', 'data_type': type_int()},
    {'name': 'link', 'description': 'Link associated with the activity, if applicable.', 'data_type': type_string()},
    {'name': 'feature_json', 'description': 'JSON containing additional feature data related to the activity. Contains the following items:', 'data_type': dbt_aql.type_json()},
    {'name': 'activity_occurrence', 'description': 'Number of times the activity occurred.', 'data_type': type_int()},
    {'name': 'activity_repeated_at', 'description': 'Timestamp of when the activity was repeated, if applicable.', 'data_type': type_timestamp()}
]
%}


    -- Check for anonymous_customer_column
  {% set stream = get_activity_config(activity).stream %}
  {%- if dbt_aql.anonymous_customer_column(stream) is not none -%}
      {%- do columns.insert(2, {'name': 'anonymous_customer_id', 'description': 'Anonymous identifier for the customer.', 'data_type': 'STRING', 'tests': ['not_null']}) -%}
  {%- endif -%}

  {% for column in columns %}
    {% do yaml_output.append('      - name: ' ~ column['name']) %}
    {% if column['name'] == 'feature_json' %}
      -- Call the macro to get feature_json items
      {% set data_types = get_activity_config(activity).data_types %}
      {% do yaml_output.append('        description: > ') %}
      {% do yaml_output.append('          ' ~ column['description']) %}
      {% for key, data_type in data_types.items() %}
        {% do yaml_output.append('            - ' ~ key ~ ': ' ~ data_type) %}
      {% endfor %}
    {% else %}
      {% do yaml_output.append('        description: "' ~ column['description'] ~ '"') %}
      {% do yaml_output.append('        data_type: ' ~ column['data_type']) %}
      {% if column['tests'] %}
        {% do yaml_output.append('        tests:') %}
        {% for test in column['tests'] %}
          {% do yaml_output.append('          - ' ~ test) %}
        {% endfor %}
      {% endif %}
    {% endif %}
  {% endfor %}

{% endfor %}


{% if execute %}
    {% set joined_yaml = yaml_output | join('\n') %}
    {{ print(joined_yaml) }}
    {% do return(joined_yaml) %}
{% endif %}

{% endmacro %}
