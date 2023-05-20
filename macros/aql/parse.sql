{% macro _strip_comments(query) %}
{%- set query_lines = modules.re.split("\n", query) -%}
{%- set query_lines_no_comments = [] -%}
{%- for line in query_lines -%}
    {%- set split_line = modules.re.split("--", line) -%}
    {%- do query_lines_no_comments.append(split_line[0]) -%}
{%- endfor -%}
{%- do return(query_lines_no_comments|join("\n")) -%}
{% endmacro %}


{% macro _clean_query(query) %}
{%- do return(modules.re.split(dbt_aql.whitespace(), query.lower().strip())|join(" ")) -%}
{% endmacro %}


{% macro parse_aql(query) %}
{%- set ws = dbt_aql.whitespace() -%}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set query_no_comments = dbt_aql._strip_comments(query) -%}
{%- set query_clean = dbt_aql._clean_query(query_no_comments) -%}
{%- set using, rest = dbt_aql._parse_keyword(query_clean, ["using"]) -%}
{%- set stream, rest = dbt_aql._parse_stream(rest) -%}
{%- set primary_activity, rest = dbt_aql._parse_activity(rest, stream, [av.select]) -%}


{%- set verb_str = "({aggregate}|{append}|{include})".format(aggregate=av.aggregate, append=av.append, include=av.include) -%}
{%- set keyword_str = ws~verb_str~ws -%}

{%- if modules.re.search(keyword_str, " "~rest) is none -%}
    {%- set num_activities = 0 -%}
{%- else -%}
    {%- set activity_starts = modules.re.findall(keyword_str, " "~rest) -%}
    {%- set num_activities = activity_starts|length -%}
{%- endif -%}
{%- set joined_activities = [] -%}
{%- set included_dataset_columns = [] -%}
{%- set rest_dict = {'rest': rest} -%}
{%- for i in range(num_activities) -%}
    {%- set verb, unused_rest = dbt_aql._parse_keyword(rest_dict.rest, [av.append, av.aggregate, av.include]) -%}
    {%- if verb in [av.append, av.aggregate] -%}
        {%- set joined_activity, x = dbt_aql._parse_activity(rest_dict.rest, stream, [av.append, av.aggregate]) -%}
        {%- do joined_activities.append(joined_activity) -%}
        {%- do rest_dict.update({'rest': x}) -%}
    {%- elif verb == av.include -%}
        {%- set included_columns, x = dbt_aql._parse_included_columns(rest_dict.rest, stream, [av.include]) -%}
        {%- for ic in included_columns -%}
            {%- do included_dataset_columns.append(ic) -%}
        {%- endfor -%}
        {%- do rest_dict.update({'rest': x}) -%}
    {%- endif -%}
{%- endfor -%}
{%- do return(namespace(
    name='parsed_aql',
    stream=stream,
    primary_activity=primary_activity,
    joined_activities=joined_activities,
    included_dataset_columns=included_dataset_columns
)) -%}
{% endmacro %}

{% macro _parse_keyword(query, keywords=none) %}
{%- set parsed_word = modules.re.split("\(", modules.re.split(dbt_aql.whitespace(), query.strip())[0].strip())[0].strip() -%}
{%- if keywords is none -%}
    {%- set parsed_word_len = parsed_word|length -%}
    {%- do return((parsed_word, query[parsed_word_len:].strip())) -%}
{%- else -%}
    {% if parsed_word not in keywords %}
        {%- set keyword_str = keywords|join(" or ") -%}
        {%- set error_message -%}
Invalid Syntax: aql query in model '{{ model.unique_id }}'
Expected {{keyword_str}}, got '{{parsed_word}}'
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- else -%}
        {%- set parsed_word_len = parsed_word|length -%}
        {%- do return((parsed_word, query[parsed_word_len:].strip())) -%}
    {% endif %}
{%- endif -%}
{% endmacro %}


{% macro _parse_stream(query) %}
{%- set stream = modules.re.split(dbt_aql.whitespace(), query)[0] %}
{%- set streams = var("dbt_aql", {}).get("streams", {}).keys() -%}
{%- if stream not in streams -%}
    {%- set error_message -%}
Error: aql query in model '{{ model.unique_id }}' specifies unconfigured stream '{{stream}}'.
The stream name should be listed as a variable in dbt_project.yml like so:
vars:
  dbt_aql:
    streams:
        {{stream}}:
        customer_id_alias: <alias>
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- else -%}
    {%- set stream_len = stream|length -%}
    {%- do return((stream, query[stream_len:].strip())) -%}
{%- endif -%}
{% endmacro %}


{% macro _parse_activity(query, stream, expected_verbs) %}
{%- set ws = dbt_aql.whitespace() -%}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set vqs = dbt_aql._valid_query_syntax() -%}
{%- set rs = vqs.relationship_selectors -%}
{%- set verb, query = dbt_aql._parse_keyword(query, expected_verbs) -%}
{%- set verb_str = "({aggregate}|{append}|{include})".format(aggregate=av.aggregate, append=av.append, include=av.include) -%}
{%- set keyword_str = ws~verb_str~ws -%}
{%- if modules.re.search(keyword_str, query) is not none -%}
    {%- set end = modules.re.search(keyword_str, query).start() -%}
    {%- set query_rest = query[end:].strip() -%}
    {%- set query = query[:end].strip() -%}
{%- else -%}
    {%- set query_rest = none -%}
{%- endif -%}

{%- if verb == av.select -%}
    {%- set join_condition = none -%}
    {%- set relationship_selector, rest = dbt_aql._parse_keyword(query, rs.select) -%}
    {%- if relationship_selector == rs.registry.nth -%}
        {%- set nth, rest = dbt_aql._parse_nth(rest) -%}
    {%- else -%}
        {%- set nth = none -%}
    {%- endif -%}

{%- elif verb == av.append -%}
    {%- set relationship_selector, rest = dbt_aql._parse_keyword(query, rs.append) -%}
    {%- if relationship_selector == rs.registry.nth -%}
        {%- set nth, rest = dbt_aql._parse_nth(rest) -%}
    {%- else -%}
        {%- set nth = none -%}
    {%- endif -%}
    {%- set jc = vqs.join_conditions.append -%}
    {%- set join_condition, rest = dbt_aql._parse_keyword(rest, jc) -%}


{%- elif verb == av.aggregate -%}
    {%- set relationship_selector = none -%}
    {%- set nth = none -%}
    {%- set jc = vqs.join_conditions.aggregate -%}
    {%- set join_condition, rest = dbt_aql._parse_keyword(query, jc) -%}

{%- endif -%}

{%- set activity_name, rest = dbt_aql._parse_keyword(rest) -%}

{%- set columns, rest = dbt_aql._parse_columns(
    query=rest,
    stream=stream,
    activity_name=activity_name,
    verb=verb,
    relationship_selector=relationship_selector,
    nth=nth,
    join_condition=join_condition
) -%}

{%- if rest is not none -%}
    {%- set extra_joins = dbt_aql._parse_extra_joins(rest) -%}
{%- else -%}
    {%- set extra_joins = none -%}
{%- endif -%}

{%- do return((dbt_aql.activity(
    activity_name=activity_name,
    stream=stream,
    verb=verb,
    join_condition=join_condition,
    relationship_selector=relationship_selector,
    columns=columns,
    nth=nth,
    extra_joins=extra_joins
), query_rest)) -%}

{% endmacro %}


{% macro _parse_nth(query) %}
{%- set opening = query[0] -%}
{%- if opening != "(" -%}
    {%- set error_message -%}
Error: relationship selector 'nth' should be followed by '('. Got '{{opening}}'
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- else -%}
    {%- set nth_raw = modules.re.split("\)", query[1:])[0] -%}
    {%- if not nth_raw.strip().isdigit() -%}
        {%- set error_message -%}
Error: relationship selector 'nth' expects numeric value. Got '{{nth_raw}}'
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- else -%}
        {%- set str_len = nth_raw|length + 2 -%}
        {%- do return((nth_raw.strip(), query[str_len:].strip())) -%}
    {%- endif -%}
{%- endif -%}
{% endmacro %}



{% macro _parse_columns(query, stream, activity_name, verb, relationship_selector, nth, join_condition) %}
{%- set ws = dbt_aql.whitespace() -%}
{%- set first_char = query.strip()[0] -%}
{%- if first_char != "(" -%}
    {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please wrap specified columns in parentheses. Expected '(', got '{{first_char}}' See:
{{ query }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}
{%- set last_char = query.strip()[-1] -%}
{%- if last_char != ")" -%}
    {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please wrap specified columns in parentheses. Expected ')', got '{{first_char}}' See:
{{ query }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- set ws_join = ws~"join"~ws -%}
{%- set query_stripped = query.strip()[1:-1] -%}

{%- set join_ixs = modules.re.search(ws_join, query_stripped) -%}
{%- if join_ixs is not none -%}
    {%- set rest = query_stripped[join_ixs.start():] -%}
    {%- set column_str = modules.re.split(",", query_stripped[:join_ixs.start()]) -%}
{%- else -%}
    {%- set rest = none -%}
    {%- set column_str = modules.re.split(",", query_stripped) -%}
{%- endif -%}


{%- set columns = [] -%}
{%- for col in column_str -%}
    {%- set parsed_col = dbt_aql._parse_column(
        column_str=col.strip(),
        stream=stream,
        activity_name=activity_name,
        verb=verb,
        relationship_selector=relationship_selector,
        nth=nth,
        join_condition=join_condition
    ) -%}
    {%- do columns.append(parsed_col) -%}
{%- endfor -%}

{%- do return((columns, rest)) -%}
{% endmacro %}



{% macro _parse_column(column_str, stream, activity_name, verb, relationship_selector, nth, join_condition) %}
{%- set punc = dbt_aql.punctuation() -%}
{%- set ws = dbt_aql.whitespace() -%}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set rs = dbt_aql._relationship_selectors() -%}
{%- set am = dbt_aql._aggregation_map() -%}

{%- if verb == av.select -%}
    {%- set aggfunc = none -%}

{%- elif verb == av.append -%}
    {%- if relationship_selector == rs.first -%}
        {%- set aggfunc = am.first_value -%}
    {%- elif relationship_selector == rs.last -%}
        {%- set aggfunc = am.last_value -%}
    {%- elif relationship_selector == rs.nth -%}
        {# arbitrary for nth #}
        {%- set aggfunc = am.last_value -%}
    {%- endif -%}

{%- elif verb == av.aggregate -%}
    {%- if modules.re.search("\(", column_str) is not none -%}
        {%- set aggfunc_str, column_str = dbt_aql._parse_keyword(column_str, am.valid_aggregations) -%}
        {%- set aggfunc = am[aggfunc_str] -%}
        {%- set column_str = column_str.translate(column_str.maketrans("","", punc)).strip() -%}
    {%- else -%}
        {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax for aggregate
activity '{{activity_name}}'. Syntax:
{{column_str}}

Hint: all columns included in an aggregated activity in a dataset must
be wrapped in a valid aggregation function.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{%- endif -%}

{%- set column = modules.re.split("(\)|{ws})".format(ws=ws), column_str)[0].strip() -%}

{%- set ws_as = ws~"as"~ws -%}
{%- if modules.re.search(ws_as, column_str) is not none -%}
    {%- set alias_base = modules.re.split(ws_as, column_str.strip())[-1] -%}
    {%- set alias = alias_base.translate(alias_base.maketrans("","", punc)).strip() -%}
{%- else -%}
    {%- set alias = dbt_aql.alias_column(activity_name, column, verb, relationship_selector, join_condition, n) -%}
{%- endif -%}

{%- do return(dbt_aql.column(
    activity_name=activity_name,
    stream=stream,
    column_name=column,
    alias=alias,
    aggfunc=aggfunc
)) -%}

{% endmacro %}

{% macro _parse_extra_joins(query) %}
{%- set ws = dbt_aql.whitespace() -%}
{%- set extra_join_list = modules.re.split(ws~"join"~ws, query) -%}
{%- if extra_join_list|length > 1 -%}
    {%- do return(extra_join_list[1:]) -%}
{%- else -%}
    {%- do return(none) -%}
{%- endif -%}
{% endmacro %}

{% macro _parse_included_columns(query, stream, expected_verbs) %}
{%- set ws = dbt_aql.whitespace() -%}
{%- set av = dbt_aql._activity_verbs() -%}
{%- set verb, query = dbt_aql._parse_keyword(query, expected_verbs) -%}
{%- set first_char = query.strip()[0] -%}
{%- if first_char != "(" -%}
    {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please wrap specified columns in parentheses. Expected '(', got '{{first_char}}' See:
{{ query }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- set verb_str = "({aggregate}|{append}|{include})".format(aggregate=av.aggregate, append=av.append, include=av.include) -%}
{%- set keyword_str = ws~verb_str~ws -%}
{%- if modules.re.search(keyword_str, query) is not none -%}
    {%- set end = modules.re.search(keyword_str, query).start() -%}
    {%- set query_rest = query[end:].strip() -%}
    {%- set query = query[:end].strip() -%}
{%- else -%}
    {%- set query_rest = none -%}
{%- endif -%}

{%- set last_char = query.strip()[-1] -%}
{%- if last_char != ")" -%}
    {%- set error_message -%}
aql query in model '{{ model.unique_id }}' has invalid syntax. Please wrap specified columns in parentheses. Expected ')', got '{{first_char}}' See:
{{ query }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- set column_str = modules.re.split(",", query[1:-1].strip()) -%}
{%- do return((column_str, query_rest)) -%}

{% endmacro %}

{% macro get_graph() %}
{% if execute %}
{{ log(graph.nodes, info=true) }}
{% endif %}
{% endmacro %}

{% macro tm(stream, aql) %}
{% set aql = config.require('aql') %}
-- depends_on: stream
{% endmacro %}