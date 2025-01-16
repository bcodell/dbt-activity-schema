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
{%- do return(modules.re.split(dbt_activity_schema.whitespace(), query.strip())|join(" ")) -%}
{% endmacro %}


{% macro parse_aql(query) %}
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set query_no_comments = dbt_activity_schema._strip_comments(query) -%}
{%- set query_clean = dbt_activity_schema._clean_query(query_no_comments) -%}
{%- set using, rest = dbt_activity_schema._parse_keyword(query_clean, ["using"]) -%}
{%- set stream, rest = dbt_activity_schema._parse_stream(rest) -%}
{%- set primary_activity, rest = dbt_activity_schema._parse_activity(rest, stream, [av.select]) -%}


{%- set verb_str = "({aggregate}|{append}|{include})".format(aggregate=av.aggregate, append=av.append, include=av.include) -%}
{%- set keyword_str = ws~verb_str~ws -%}

{%- if modules.re.search(keyword_str, " "~rest, modules.re.IGNORECASE) is none -%}
    {%- set num_activities = 0 -%}
{%- else -%}
    {%- set activity_starts = modules.re.findall(keyword_str, " "~rest.lower()) -%}
    {%- set num_activities = activity_starts|length -%}
{%- endif -%}
{%- set joined_activities = [] -%}
{%- set included_dataset_columns = [] -%}
{%- set rest_dict = {'rest': rest} -%}
{%- for i in range(num_activities) -%}
    {%- set verb, unused_rest = dbt_activity_schema._parse_keyword(rest_dict.rest, [av.append, av.aggregate, av.include]) -%}
    {%- if verb in [av.append, av.aggregate] -%}
        {%- set joined_activity, x = dbt_activity_schema._parse_activity(rest_dict.rest, stream, [av.append, av.aggregate]) -%}
        {%- do joined_activities.append(joined_activity) -%}
        {%- do rest_dict.update({'rest': x}) -%}
    {%- elif verb == av.include -%}
        {%- set included_columns, x = dbt_activity_schema._parse_included_columns(rest_dict.rest, stream, [av.include]) -%}
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
{%- set parsed_word = modules.re.split("\(", modules.re.split(dbt_activity_schema.whitespace(), query.lower().strip())[0].strip())[0].strip() -%}
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
{%- set stream = modules.re.split(dbt_activity_schema.whitespace(), query)[0] %}
{%- set streams = var("dbt_activity_schema", {}).get("streams", {}).keys() -%}
{%- if stream not in streams -%}
    {%- set error_message -%}
Error: aql query in model '{{ model.unique_id }}' specifies unconfigured stream '{{stream}}'.
The stream name should be listed as a variable in dbt_project.yml like so:
vars:
  dbt_activity_schema:
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
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set vqs = dbt_activity_schema._valid_query_syntax() -%}
{%- set rs = vqs.relationship_selectors -%}
{%- set verb, query = dbt_activity_schema._parse_keyword(query, expected_verbs) -%}
{%- set verb_str = "({aggregate}|{append}|{include})".format(aggregate=av.aggregate, append=av.append, include=av.include) -%}
{%- set keyword_str = ws~verb_str~ws -%}
{%- if modules.re.search(keyword_str, query, modules.re.IGNORECASE) is not none -%}
    {%- set end = modules.re.search(keyword_str, query, modules.re.IGNORECASE).start() -%}
    {%- set query_rest = query[end:].strip() -%}
    {%- set query = query[:end].strip() -%}
{%- else -%}
    {%- set query_rest = none -%}
{%- endif -%}

{%- if verb == av.select -%}
    {%- set join_condition = none -%}
    {%- set relationship_selector, rest = dbt_activity_schema._parse_keyword(query, rs.select) -%}
    {%- if relationship_selector == rs.registry.nth -%}
        {%- set nth, rest = dbt_activity_schema._parse_nth(rest) -%}
    {%- else -%}
        {%- set nth = none -%}
    {%- endif -%}
    {%- if relationship_selector == rs.registry.time_spine -%}
        {%- set interval, end_period, rest = dbt_activity_schema._parse_time_spine(rest) -%}
    {%- else -%}
        {%- set interval = none -%}
        {%- set end_period = none -%}
    {%- endif -%}


{%- elif verb == av.append -%}
    {%- set relationship_selector, rest = dbt_activity_schema._parse_keyword(query, rs.append) -%}
    {%- if relationship_selector == rs.registry.nth -%}
        {%- set nth, rest = dbt_activity_schema._parse_nth(rest) -%}
    {%- else -%}
        {%- set nth = none -%}
    {%- endif -%}
    {%- set jc = vqs.join_conditions.append -%}
    {%- set join_condition, rest = dbt_activity_schema._parse_keyword(rest, jc) -%}


{%- elif verb == av.aggregate -%}
    {%- set relationship_selector = none -%}
    {%- set nth = none -%}
    {%- set jc = vqs.join_conditions.aggregate -%}
    {%- set join_condition, rest = dbt_activity_schema._parse_keyword(query, jc) -%}

{%- endif -%}

{%- set activity_name, rest = dbt_activity_schema._parse_keyword(rest) -%}

{%- set columns, rest = dbt_activity_schema._parse_columns(
    query=rest,
    stream=stream,
    activity_name=activity_name,
    verb=verb,
    relationship_selector=relationship_selector,
    nth=nth,
    join_condition=join_condition
) -%}

{%- if rest is not none -%}
    {%- set filters, rest = dbt_activity_schema._parse_filters(rest) -%}
{%- else -%}
    {%- set filters = none -%}
{%- endif -%}


{%- if rest is not none -%}
    {%- set extra_joins = dbt_activity_schema._parse_extra_joins(rest) -%}
{%- else -%}
    {%- set extra_joins = none -%}
{%- endif -%}

{%- do return((dbt_activity_schema.activity(
    activity_name=activity_name,
    stream=stream,
    verb=verb,
    join_condition=join_condition,
    relationship_selector=relationship_selector,
    columns=columns,
    nth=nth,
    filters=filters,
    extra_joins=extra_joins,
    interval=interval,
    end_period=end_period
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


{% macro _parse_time_spine(query) %}
{%- set opening = query[0] -%}
{%- if opening != "(" -%}
    {%- set error_message -%}
Error: relationship selector 'time_spine' should be followed by '('. Got '{{opening}}'
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- set split_string = modules.re.split("\)", query[1:]) -%}
{%- set args_raw = split_string[0] -%}
{%- set rest = split_string[1].strip()~')' -%}
{%- set args_split = modules.re.split(",", args_raw) -%}
{%- set arg_split_len = args_split|length -%}
{%- set arg_dict = {} -%}

{%- if args_split|length != 2 -%}
{%- set error_message -%}
Error: relationship selector 'time_spine' should have two arguments, 'interval', and 'end_period'. Got arguments '{{args_raw}}'
{%- endset -%}
{{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- for arg in args_split -%}
{%- set split_arg = modules.re.split("=", arg) -%}
{%- set kw = split_arg[0].strip() -%}

{%- if kw not in ["interval", "end_period"] -%}
{%- set error_message -%}
Error: relationship selector 'time_spine' argument should explicitly specify arguments 'interval', and 'end_period'. Got '{{kw}}'.
{%- endset -%}
{{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- set val = split_arg[1].strip() -%}
{%- if kw == "interval" -%}
    {%- set intervals = ["day", "week", "month", "quarter", "year"] -%}

    {%- if val not in intervals -%}
    {%- set error_message -%}
Error: relationship selector 'time_spine' argument 'interval' expects one of '{{intervals}}'. Got '{{val}}'.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {%- do arg_dict.update({"interval": val}) -%}
{%- elif kw == "end_period" -%}
    {%- set end_periods = ["max", "current"] -%}

    {%- if val not in end_periods -%}
    {%- set error_message -%}
Error: relationship selector 'time_spine' argument 'end_period' expects one of '{{end_periods}}'. Got '{{val}}'.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
    {%- do arg_dict.update({"end_period": val}) -%}

{%- else -%}
    {%- set error_message -%}
Error: relationship selector 'time_spine' expects arguments 'interval', and 'end_period' expects one of '{{end_periods}}'. Got '{{val}}'.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
{%- endif -%}

{%- endfor -%}
{%- do return((arg_dict["interval"], arg_dict["end_period"], rest)) -%}
{% endmacro %}



{% macro _parse_columns(query, stream, activity_name, verb, relationship_selector, nth, join_condition) %}
{%- set ws = dbt_activity_schema.whitespace() -%}
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

{%- set ws_keyword = ws~"(filter|join)"~ws -%}
{%- set query_stripped = query.strip()[1:-1] -%}

{%- set keyword_ixs = modules.re.search(ws_keyword, query_stripped, modules.re.IGNORECASE) -%}
{%- if keyword_ixs is not none -%}
    {%- set rest = query_stripped[keyword_ixs.start():] -%}
    {%- set column_str = modules.re.split(",", query_stripped[:keyword_ixs.start()]) -%}
{%- else -%}
    {%- set rest = none -%}
    {%- set column_str = modules.re.split(",", query_stripped) -%}
{%- endif -%}


{%- set columns = [] -%}
{%- for col in column_str -%}
    {%- set parsed_col = dbt_activity_schema._parse_column(
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
{%- set punc = dbt_activity_schema.punctuation() -%}
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set rs = dbt_activity_schema._relationship_selectors() -%}
{%- set am = dbt_activity_schema._aggregation_map() -%}

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
    {%- if modules.re.search("\(", column_str, modules.re.IGNORECASE) is not none -%}
        {%- set aggfunc_str, column_str = dbt_activity_schema._parse_keyword(column_str, am.valid_aggregations) -%}
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
{%- if modules.re.search(ws_as, column_str, modules.re.IGNORECASE) is not none -%}
    {%- set alias_base = modules.re.split(ws_as, column_str.strip(), modules.re.IGNORECASE)[-1] -%}
    {%- set alias = alias_base.translate(alias_base.maketrans("","", punc)).strip() -%}
{%- else -%}
    {%- set alias = dbt_activity_schema.alias_column(activity_name, column, verb, relationship_selector, join_condition, n) -%}
{%- endif -%}

{%- do return(dbt_activity_schema.column(
    activity_name=activity_name,
    stream=stream,
    column_name=column,
    alias=alias,
    aggfunc=aggfunc
)) -%}

{% endmacro %}

{% macro _parse_filters(query) %}
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set ws_join = ws~"join"~ws -%}
{%- set query_stripped = ' '~query.strip() -%}

{%- set join_ixs = modules.re.search(ws_join, query_stripped, modules.re.IGNORECASE) -%}
{%- if join_ixs is not none -%}
    {%- set rest = query_stripped[join_ixs.start():] -%}
    {%- set query = query_stripped[:join_ixs.start()] -%}
{%- else -%}
    {%- set rest = none -%}
    {%- set query = query_stripped -%}
{%- endif -%}

{%- set filter_list = modules.re.split(ws~"filter"~ws, " "~query) -%}
{%- if filter_list|length > 1 -%}
    {%- set filters = filter_list[1:] -%}
{%- else -%}
    {%- set filters = none -%}
{%- endif -%}
{%- do return((filters, rest)) -%}
{% endmacro %}


{% macro _parse_extra_joins(query) %}
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set extra_join_list = modules.re.split(ws~"join"~ws, " "~query) -%}
{%- if extra_join_list|length > 1 -%}
    {%- do return(extra_join_list[1:]) -%}
{%- else -%}
    {%- do return(none) -%}
{%- endif -%}
{% endmacro %}

{% macro _parse_included_columns(query, stream, expected_verbs) %}
{%- set ws = dbt_activity_schema.whitespace() -%}
{%- set av = dbt_activity_schema._activity_verbs() -%}
{%- set verb, query = dbt_activity_schema._parse_keyword(query, expected_verbs) -%}
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
{%- if modules.re.search(keyword_str, query, modules.re.IGNORECASE) is not none -%}
    {%- set end = modules.re.search(keyword_str, query, modules.re.IGNORECASE).start() -%}
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
