# **dbt-aql**
A dbt package to query Activity Streams using a SQL-esque interface called `aql` - Activity Query Language. In addition to a proposed implementation of aql, it contains myriad utility functions to empower users to build entire Activity Schema data pipelines in their dbt projects.

# **Motivation**
After contributing to multiple dbt projects that support Activity Schema modeling in dbt, it became apparent that a macro-based UX for generating ad hoc datasets from Activity Stream tables for analysis development left a lot to be desired. This project exists for two reasons - to make it easier to operate an Activity Schema, and to offer a preliminary proposal for a standard method for deriving datasets from stream-like tables.

For one, macro interfaces are fickle, as developers need to memorize the input arguments for the macro and how each should be provided. is the input a string? A dbt `ref`? Another macro? Rendered or unrendered? This challenge compounds on the existing need for developers to learn the semantics of Activity Schema mdoeling even before leveraging a tool to build it.

Second, all versions of this macro are particularly verbose - declaring a stream, a primary activity, one or more joined activities, their relationships to the primary activity, columns to include, aliases and aggregations to apply to each column - defining all of these in a macro gets redundant, and the code gets less and less readable as the dataset is expanded.

But ultimately, there's an emotional component as well. Data professionals _enjoy_ writing SQL. They're familiar with it. They're good at it. It's easy to achieve a flow state while writing it. But the UX of a dbt macro is decidedly un-SQL-like. Developers embrace macros when it allows them to make their SQL more DRY - but to provide a solution that entirely strips away any SQL writing from their work, particularly given the aforementioned challenges, feels like its missing the mark.

Given all of these shortcomings, It wouldn't be surprising if developers could write bespoke SQL against an Activity Stream table faster than they can populate an unfamiliar dbt macro. However, writing that SQL from scratch is cumbersome - lots of redundant self-joins and filters - and cumbersome enough to give developers pause from adopting this modeling paradigm.

This project offers a middle ground. With aql, developers can derive their datasets from Activity Streams in a way that is less tedious than writing traditional SQL, easier to learn and read than a convoluted macro, and above all, it _feels_ like writing SQL (with a few new keywords).

# **Inspiration**
Numerous projects influenced the design of this one. Such projects include:
- [Activity Schema Spec](https://github.com/ActivitySchema/ActivitySchema)
- [Prql](https://prql-lang.org/)
- The [activity schema dbt package](https://github.com/tnightengale/dbt-activity-schema) from @tnightengale
- My own prior implementation of an [activity schema dbt package](https://github.com/bcodell/dbt-activity-schema)


# **Implementation**
At a high level, all this project does is parse a string, generate a set of parameters from that string, then render a SQL query from those parameters. But at its core, this project is effectively a proposal for a vocabulary to use when querying stream-like tables. In order to obtain meaningful feedback and iterate on such a proposal, it needs to meet its prospective users where they already live. In the analytics world, that place is dbt.

Given that need, there were a couple paths forward for building this project. One is to build a standalone package, with integrations for use in other dialects, languages, and frameworks. Prql is a great example of a similar project. But while [admirable implementations exist](https://github.com/PRQL/dbt-prql) to integrate such projects with dbt, a standard integration pattern has yet to be agreed upon, and taking such a path would require more upfront work to support multiple SQL dialects. The other approach - a dbt package - has a known standard for integrating with dbt, a prototype could be built rather quickly, and it could leverage the multi-dialect support that comes for free in dbt. For those reasons, that was the solution picked for this iteration of the project.

Finally, given the nascent (but burgeoning) state of adoption of Activity Schema as a viable data modeling standard, an aql query parser alone isn't enough. People need to adopt the modeling paradigm before they spend time thinking about the ideal semantics for using it. For that reason, this project also contains all the relevant utility functions needed to build entire Activity Schema data pipelines with dbt.
</br></br>


# **Project Configuration**
In order to use the package, a variable called `dbt_aql` needs to be added to the project's `dbt_project.yml`. It should take the following format:
``` yml
vars:
  dbt_aql:
    column_configuration:
      included_optional_columns:
        - revenue_impact
        - link
      column_aliases:
        feature_json: attributes
    streams:
      stream_1:
        customer_id_alias: entity_uuid
        anonymous_customer_id_alias: anonymous_entity_uuid
        model_prefix: prefix_1__
      stream_2:
        customer_id_alias: entity_uuid
        model_prefix: prefix_2__
```

## **Project-Level Column Configurations**
Some configurations are asserted at a project level and apply to all Activity Schema pipelines in a project. They are all related to columns included in activity and stream models, and should be nested in a second-level variable called `column_configuration`. They are:
</br></br>

### **`included_optional_columns`**
_Description_: This variable takes a list of 0 or more columns defined as optional by the Activity Schema v2 spec. Valid options include:
- `link`
- `revenue_impact`

_Required_: No
</br></br>

### **`column_aliases`**
_Description_: This variable takes 0 or more nested variables, where each variable key corresponds to the standard column names as specified by the Activity Schema v2 spec:
- `activity_id`
- `ts`
- `activity`
- `feature_json`
- `link` (must be explicitly included in `included_optional_columns` above as well)
- `revenue_impact` (must be explicitly included in `included_optional_columns` above as well)

_Required_: No
</br></br>

## **Stream-Level Configurations**
Some configurations are unique to each Activity Schema instance.

### **`streams`**
_Description_: This variable acts as a registry of each Activity Schema data pipeline in the project. It should contain nested variables, where each variable name corresponds to the model name of each Activity Stream model in the project. In the example above, the project should contain two Activity Stream models: `stream_1` and `stream_2`.

_Required_: Yes

Nested under each registered stream are the following variables:
</br></br>

#### **`customer_id_alias`**
_Description_: This variable takes a string that represents the alias that will be used in lieu of the standard `customer` column in the Activity Schema spec, which represents the canonical ID of the entity represented by the stream.

_Required_: Yes
</br></br>

#### **`anonymous_customer_id_alias`**
_Description_: This variable takes a string that represents the alias that will be used in lieu of the standard `anonymous_customer_id` column in the Activity Schema spec, which represents the anonymous ID of the entity represented by the stream. 
> **Note: This is an optional column according to the Activity Schema v2 Spec. If this variable is excluded, then the Activity and Activity Stream models for the corresponding Activity Schema will exclude `anonymous_customer_id` as a column.**

_Required_: No
</br></br>

#### **`model_prefix`**
_Description_: This variable takes a string that represents the prefix used for the names of all models associated with the corresponding Activity Schema, as it is a common practice to duplicate an activity across multiple streams, and to distinguish with a prefix in the model name itself. This variable is used for aliasing purposes downstream to improve readability of columns generated in the dataset.

_Required_: No
</br></br>

# **Activities**
Each Activity Schema should have 1 dbt model per Activity. The model name should be the name of the Activity, and is the string that will be populated in the standard `activity` column. It is recommended to assert a naming convention standard for all activities for sake of consistency.
</br></br>

## **Configuration**
The following variables should be added to the model's `config`, either in the model's `yml` definition or in the `config` block within the model itself.

An example configuration in the model `config` block looks like the following:
```sql
{{
    config(
        materialized='table',
        stream='stream_1',
        data_types={
            'feature_1': type_int(),
            'feature_2': type_string(),
            'feature_3': type_timestamp(),
        }
    )
}}
```

An example configuration in the model `yml` definition looks like the following:
```yml
version: 2

models:

  - name: prefix_1__activity_name
    config:
      stream: stream_1
      data_types:
        feature_1: int
        feature_2: string
        feature_3: timestamp
```

**Note: It is highly recommended to use the first option, as it better supports with cross-database functionality.**
</br></br>


### **`stream`**
_Description_: The model name of the Activity Stream model in the corresponding Activity Schema pipeline. Should correspond with the name of one of the streams registered in the `dbt_project.yml`

_Required_: Yes
</br></br>

### **`data_types`**
_Description_: A registry of Activity-specific columns. Expects a dictionary where each key is the alias of the column and each value is that column's data type. This information is used to automatically build the JSON object for the `feature_json` column in the Activity models, and to properly typecast columns when extracting them from the `feature_json` column when building datasets. If an Activity has no custom columns, this parameter should be excluded from the configuration.

_Required_: No
</br></br>

## **Code**
Each Activity model should look similar to the following:
```sql
with base as (
    select
        entity_id as {{dbt_aql.customer_column('stream_1')}},
        created_at as {{dbt_aql.schema_columns().ts}},
        feature_1 as feature_1_alias,
        feature_2 as feature_2_alias,
        ...
    from {{ ref('my_dbt_table') }}
)
{{ dbt_aql.build_activity(
    cte='base'
)}}
```
The `build_activity` macro is a convenience function that will take the data from the CTE that's passed as an argument and shape it into the standardized schema that the user has configured for the Activity model's corresponding Activity Schema. The query itself can have as much or as little business logic as necessary to define the Activity. The only expectation is that the CTE that is passed to the macro contains the following:
* A column that is aliased to the corresponding Activity Schema's `customer` column alias. This alias can be asserted programmatically with the jinja snippet `{{dbt_aql.customer_column('stream_1')}}` as seen in the example code, or manually with code. This column will be cast as a string.
* A column that can be cast as a timestamp that is aliased to the `ts` column alias as configured for all Activity Schema pipelines in the project. This alias can be asserted programmatically with the jinja snippet `{{dbt_aql.schema_columns().ts}}` as seen in the example code, or manually with code. This column will be cast as a timestamp.

All other columns will be added automatically and aliased as configured in the project to the final `select` statement that is generated by the `build_activity` macro.
</br></br>

# **Streams**
Each Activity Schema should have exactly 1 stream model. The model should be the name of the stream that is registered in the `streams` variable in `dbt_project.yml`.

## **Configuration**
In order to maximize computational performance when querying a stream in downstream models, it is recommended to use the clustering/partition keys as recommended by the spec. A convenience macro has been provided with adapter-specific implementations to make this easy for users. To use, simply pass the `dbt_aql.cluster_keys()` to the appropriate argument in the model config block.

Example implementations of this configuration for supported warehouses are provided below:
```sql
-- Snowflake
{{
    config(
        cluster_by=dbt_aql.cluster_keys()
    )
}}

-- Bigquery
{{
    config(
        partition_by=dbt_aql.cluster_keys().partition_by,
        cluster_by=dbt_aql.cluster_keys().cluster_by
    )
}}

-- Redshift
{{
    config(
        sort=dbt_aql.cluster_keys().sort,
        dist=dbt_aql.cluster_keys().dist
    )
}}

-- DuckDB
{{
    config(
        options={"partition_by": dbt_aql.cluster_keys()}
    )
}}

```

</br></br>

## **Code**
Each Activity model should look similar to the following:
```sql
{{ dbt_aql.build_stream(
    activity_list=[
        ref('prefix_1__activity_1'),
        ref('prefix_1__activity_2')
    ]
) }}
```
The `build_stream` macro is a convenience function that takes as input a list of `ref`'d activity models and creates a unioned representation of them. If this model's materialization is specified as incremental, then for each activity, it will only insert rows with a `ts` value greater than the maximum `ts` value in the stream table for that activity.
</br></br>

# **Creating Datasets: Querying The Activity Stream with `aql`**
The Activity Stream is the entrypoint for creating any denormalized dataset needed for analysis or reporting. However, as discussed above, writing bespoke SQL to query it in the standard pattern is tedious and error-prone, and using a macro to auto-generate the SQL is brittle and a subpar development experience.

To solve these shortcomings, this project introduces `aql` (Activity Query Language) - an interface that's easier to learn and more SQL-esque than a macro but substantially more concise than bespoke SQL.

Under the hood, this package will parse the `aql` query string into a json object, then use the object parameters to render the appropriate SQL statement.
</br></br>

## **The Anatomy of an `aql` Query**
As a refresher, Activity Stream queries require the following inputs to be defined:
* The Activity Stream table to use
* A Primary Activity with columns to include. The Primary Activity defines the granularity of the dataset.
* 0 or more Joined Activities, with Temporal Join criteria defined and columns to include, with specific aggregation functions to use (if applicable) for each column.

All of these inputs are still needed, and they can be viewed in the following example `aql` query:
```sql
using stream_1
select all activity_1 (
    ts as activity_1_at,
    feature_1,
    feature_2
)
append first ever activity_2 (
    ts as first_ever_activity_2_at
)
aggregate after activity_3 (
    count(activity_id) as count_activity_3_after
    sum(feature_x) as sum_feature_x_after
)
```

To use in a dbt model, assign the aql query to a variable, and pass it to the `dataset` macro like so:
```sql
{% set aql %}
using stream_1
select all activity_1 (
    ts as activity_1_at,
    feature_1,
    feature_2
)
append first ever activity_2 (
    ts as first_ever_activity_2_at
)
aggregate after activity_3 (
    count(activity_id) as count_activity_3_after
    sum(feature_x) as sum_feature_x_after
)
{% endset %}

{{ dbt_aql.dataset(aql) }}
```

Implementation details are provided below.
</br></br>

## **Activity Stream**
In order to specify which Activity Stream to use, each `aql` query expects to begin with the following syntax:
```sql
using <stream_name>
```

### **Syntax and Parameters**
_`using`_</br>
The verb that specifies which Activity Stream to use when building the dataset. This verb must be the opening token of every `aql` query, or an error will be thrown.

`<stream_name>`</br>
The model name of the stream in the dbt project. Under the hood, this stream will be `ref'd` in the SQL code derived by the package so that model lineage is maintained.
</br></br>

## **Primary Activity**
In order to specify which Activity should be designated as primary, the following syntax should be used:
```sql
select <occurrence_selector> <activity_name> (
    <column>[ as <alias>],
    <column>[ as <alias>]
)
```
### **Syntax and Parameters**
_`select`_</br>
The verb that specifies which Activity will be used as the Primary Activity when building the dataset. The word that immediately follows the Activity Stream clause in the `aql` query must be `select`, or an error will be thrown.
</br></br>

_`<occurrence_selector>`_</br>
The second word in the clause specifies which occurrence(s) of the Activity to include in the dataset. Valid options are:
* `all` - all occurrences of the Activity for each Entity
* `first` - the first occurrence of the Activity for each Entity
* `last` - the last occurrence of the Activity for each Entity
* `nth(n)` - the n-th occurrence of the Activity for each Entity (e.g. 2nd, 3rd, etc)
    * If `nth` is specified as the selector, the query expects to receive a numeric value within parentheses. For example, to select the 2nd occurrence of an activity, the selector syntax should be `nth(2)`
</br></br>

`<activity_name>`</br>
The last word in the clause should represent the name of an activity that exists in the Activity Stream being queried. It can include or exclude the prefix defined in the project configuration, but it should be passed as a single word without whitespaces.
</br></br>

_`<column>[ as <alias>]`_</br>
For each Activity, the columns to include must be explicitly defined within parentheses. Each `<column>` value must correspond to the alias of one of the standard Activity Schema columns as configured in `dbt_project.yml`. Any `<column>` parsed that is not identified as such is assume to be one of the keys in the `feature_json` column for the Activity, and will be extracted accordingly.

Each included column can optionally be aliased to a whitespace-free friendly name `as <alias>`, where `<alias>` is the name of the column that will be applied to the dataset. Each defined alias must be preceded by `as`. Aliasing is optional - if no alias is explicitly defined, an automated alias will be applied to the column. See more in the `alias_column` macro.
</br></br>

## **Joined Activities**
There are two methods of joining an Activity to the primary Activity in a dataset: `append` and `aggregate`. The join method determines the syntax used for the `aql` query.
</br></br>

## **Append**
Joining an Activity via the `append` method effectively means to retrieve the column values of a specific occurrence of the Joined Activity.
```sql
append <occurrence_selector> <join_condition> <activity_name> (
    <column>[ as <alias>],
    <column>[ as <alias>]
)
```
</br>

### **Syntax and Parameters**
_`append`_</br>
The verb that specifies that the Activity will be joined to the Primary Activity via the `append` join method when building the dataset.
</br></br>

_`<occurrence_selector>`_</br>
The next word in the clause specifies which occurrence of the Activity to include in the dataset from the set of Joined Activities. Valid options are:
* `first` - the first occurrence of the Activity for each Entity, predicated on the join condition specified
* `last` - the last occurrence of the Activity for each Entity, predicated on the join condition specified
* `nth(n)` - the n-th occurrence of the Activity for each Entity (e.g. 2nd, 3rd, etc).
    * For Appended Activities, `nth` always attempts to retrieve the n-th overall occurrence of the Joined Activity, regardless of the join condition specified
    * If `nth` is specified as the selector, the query expects to receive a numeric value within parentheses. For example, to select the 2nd occurrence of an activity, the selector syntax should be `nth(2)`

> **Note: `all` is not a valid `occurrence_selector` for Appended Activities, because the `append` method for Joined Activites is designed to retrieve the specific column values of at most one occurrence of the Joined Activity. It is only a valid selector for the Primary Activity. Specifying `all` as the `occurrence_selector` for an Appended Activity will throw an error.**

</br>

_`<join_condition>`_</br>
The next word in the clause specifies how the Activity will be joined to the Primary. Valid options are:
* `after` - join the set of Joined Activity occurrences that occur after the corresponding Primary Activity occurrence
* `before` - join the set of Joined Activity occurrences that occur before the corresponding Primary Activity occurrence
* `between` - join the set of Joined Activity occurrences that occur after the corresponding Primary Activity occurrence and before the next Primary Activity occurrence
* `ever` - join the set of Joined Activity occurrences from the full set of Appended Activities, independent of the timestamp of the corresponding Primary Activity.
</br></br>

`<activity_name>`</br>
The next word in the clause should represent the name of an activity that exists in the Activity Stream being queried. It can include or exclude the prefix defined in the project configuration, but it should be passed as a single word without whitespaces.

_`<column>[ as <alias>]`_</br>
For each Activity, the columns to include must be explicitly defined within parentheses. Each `<column>` value must correspond to the alias of one of the standard Activity Schema columns as configured in `dbt_project.yml`. Any `<column>` parsed that is not identified as such is assume to be one of the keys in the `feature_json` column for the Activity, and will be extracted accordingly.

Each included column can optionally be aliased to a whitespace-free friendly name `as <alias>`, where `<alias>` is the name of the column that will be applied to the dataset. Each defined alias must be preceded by `as`. Aliasing is optional - if no alias is explicitly defined, an automated alias will be applied to the column. See more in the `alias_column` macro.
</br></br>

## **Aggregate**
Joining an Activity via the `aggregate` method effectively means to retrieve the column values of multiple occurrences of the Joined Activity, and roll them up via an aggregation function.

```sql
aggregate <join_condition> <activity_name> (
    <aggfunc>(<column>)[ as <alias>],
    <aggfunc>(<column>)[ as <alias>]
)
```
</br>

### **Syntax and Parameters**
_`aggregate`_</br>
The verb that specifies that the Activity will be joined to the Primary Activity via the `aggregate` join method when building the dataset.
</br></br>

_`<join_condition>`_</br>
The next word in the clause specifies how the Activity will be joined to the Primary. Valid options are:
* `after` - join the set of Joined Activity occurrences that occur after the corresponding Primary Activity occurrence
* `before` - join the set of Joined Activity occurrences that occur before the corresponding Primary Activity occurrence
* `between` - join the set of Joined Activity occurrences that occur after the corresponding Primary Activity occurrence and before the next Primary Activity occurrence
* `all` - join the set of Joined Activity occurrences from the full set of Appended Activities, independent of the timestamp of the corresponding Primary Activity.
> **Note: `all` and `ever` join conditions have the same effect, but `all` is used for `aggregate` joins and `ever` is used for `append` joins for sake of semantic clarity when reading `aql`.**
</br></br>

`<activity_name>`</br>
The next word in the clause should represent the name of an activity that exists in the Activity Stream being queried. It can include or exclude the prefix defined in the project configuration, but it should be passed as a single word without whitespaces.
</br></br>

_`<aggfunc>(<column>)[ as <alias>]`_</br>
For each Activity, the columns to include must be explicitly defined within parentheses.

For Activities joined by the `aggregate` method, **_every column_** needs to have an aggregation function specified. The following aggregation functions are supported:
* `count` - returns a count of rows for the specified column from the set of joined rows
* `count_distinct` - returns a count of unique values in the specified column from the set of joined rows
* `first_value` - returns the first value of the specified column from the set of joined rows
* `is_null` - returns `true` if any value of the specified column in the set of joined rows is null, otherwise returns `false` (i.e. if all column values are not null)
* `last_value` - returns the last value of the specified column from the set of joined rows
* `listagg` - returns a concatenated string of all values (including duplicates) in the specified column from the set of joined rows, delimited by a linebreak
* `listagg_distinct` - returns a concatenated string of all unique values in the specified column from the set of joined rows, delimited by a linebreak
* `max` - returns the maximum value of the specified column from the set of joined rows
* `min` - returns the minimum value of the specified column from the set of joined rows
* `not_null` - returns `true` if any value of the specified column in the set of joined rows is not null, otherwise returns `false` (i.e. if all column values are null)
* `sum_bool` - transforms boolean values from `(true, false)` to `(1, 0)` from the specified column and returns a sum from the set of joined rows. **_Specified column must be data type boolean to use._**
* `sum` - returns a sum of the specified column from the set of joined rows. **_Specified column must be data type numeric (e.g. integer, float) to use._**


Each `<column>` value must correspond to the alias of one of the standard Activity Schema columns as configured in `dbt_project.yml`. Any `<column>` parsed that is not identified as such is assume to be one of the keys in the `feature_json` column for the Activity, and will be extracted accordingly.

Each included column can optionally be aliased to a whitespace-free friendly name `as <alias>`, where `<alias>` is the name of the column that will be applied to the dataset. Each defined alias must be preceded by `as`. Aliasing is optional - if no alias is explicitly defined, an automated alias will be applied to the column. See more in the `alias_column` macro.
</br></br>

# **Advanced Usage**

## **Extra Join Criteria for Joined Activities**
Example code:
```sql
select all primary_activity (
    activity_id as primary_activity_id,
    ts as primary_activity_at,
    product_type as primary_activity_product_type
)
-- first joined activity
aggregate after activity_1 (
    count(activity_id) as count_activity_1_within_1_hour_after
    sum(feature_x) as sum_feature_x_within_1_hour_after
    -- join after primary activity as long as it was within 30-60 minutes of primary activity
    -- each criteria instantiated with a join, no "and"
    join {joined}.{ts} > dateadd('minutes', 30, {primary}.{ts})
    join {joined}.{ts} < dateadd('minutes', 60, {primary}.{ts})
)
-- second joined activity
append first after activity_2 (
    ts as product_type_activity_2_occurred_at
    -- join to primary activity as long as product_type value for activity_2 is equal to product_type value for primary_activity
    join json_extract_path_text({joined}.{feature_json}, 'product_type') = {primary}.primary_activity_product_type
)
```
Joined Activities can be further refined to the Primary Activity via extra join criteria. This functionality is useful for building datasets that represent subprocesses within a given Activity Schema (e.g. application-specific activities in a `job_stream` for recruiting data). This functionality is achieved by using the `join` keyword after all specified columns in the column selection block for a given Joined Activity. It is not applicable to Primary Activities; including this clause will be ignored. Each join criteria should be initialized with a `join` clause (no `and` clause used). It should be specified after the `filter` clause (if one exists in the statement). Each declared clause should relate a column from the Primary Activity to a column in the Joined Activity.


This feature supports f-string functionality for the following keywords:
* `joined` - will alias to the appropriate CTE alias for the Joined Activity when the aql is transpiled into SQL
* `primary` - will alias to the appropriate CTE alias for the Primary Activity when the aql is transpiled into SQL
* All of the default column names from the Activity Schema spec will be transformed to their appropriate aliases as defined in the project configuration (or the alias can be explicitly stated)

> **Note: Any column included with the Primary Activity will be available to be used in the extra join criteria for a Joined Activity. It can be referenced via its alias and does not need to be referenced via f-string syntax (see syntax in second joined activity from example code above for reference).**

</br>

> ⚠️ **WARNING: Do not use `{primary}` for custom logic for Joined Activities using the `aggregate` join method and the `all` join clause. For performance purposes, these Joined Activities are not actually joined back to the Primary Activity when deriving their aggregated columns, so there will be no `{primary}` CTE to join to, and a SQL error will be thrown.**

</br>

## **Defining Canonical Dataset Columns**
Since some columns from joined activities will represent canonical definitions for certain business values or metrics (e.g. `ts` from `append first ever created_account` is the agreed-upon business definition of when an account became active), it's important to have a means to persist that definition and reference it by its namespaced value. That is done in two steps - the `dataset_column` custom materialization and the `include` join method in aql. It is implemented as follows:

### `dataset_column` materialization
```sql
-- filename customer__total_items_purchased_after.sql

{% set aql %}
using customer_stream
aggregate after bought_something (
    sum(total_items_purchased)
)
{% endset %}

{{config(materialized='dataset_column', aql=aql)}}

{{ dbt_aql.dataset_column(aql) }}
```

### `include` join method
```sql
include (
    total_items_purchased_after -- same as filename of definition, can include or exclude configured stream prefix
)
```

</br>

## **Filtering Activities**
Activities can effectively be sub-classed using the `filter` clause. This functionality is particularly useful for ease of development and exploratory analysis of specific subsets of activities, instead of having to create a duplicated version of that activity. Filters can be applied to Primary Activities or Joined Activities. They should be included in the column selection block, after the columns and before the extra join clauses (if any). Each filter clause should be instantiated with a `filter` key.


This feature supports f-string functionality for the following keywords:
* `joined` - will alias to the appropriate CTE alias for the Joined Activity when the aql is transpiled into SQL
* `primary` - will alias to the appropriate CTE alias for the Primary Activity when the aql is transpiled into SQL
* All of the default column names from the Activity Schema spec will be transformed to their appropriate aliases as defined in the project configuration (or the alias can be explicitly stated)

> ⚠️ **WARNING: Do not use `{primary}` for custom logic for Joined Activities using the `aggregate` join method and the `all` join clause. For performance purposes, these Joined Activities are not actually joined back to the Primary Activity when deriving their aggregated columns, so there will be no `{primary}` CTE to join to, and a SQL error will be thrown.**

</br>

> ⚠️ **WARNING: Columns contained in `feature_json` must be explicitly extracted and typecast. This package includes the convenience function `json_extract` to help - see example below for syntax specifics.**

</br>

Example code:
```sql
select first visited_page (
    activity_id as activity_id,
    entity_uuid as customer_id,
    ts as first_visited_google_at
    -- json_extract will be rendered appropriately based on the target
    -- keys passed to json_extract should be wrapped in quotes
    filter {{dbt_aql.json_extract('{feature_json}', 'referrer_url')}} = 'google.com'
)
append first ever visited_page (
    ts as first_2023_page_visit_at
    filter {ts} >= '2023-01-01'
)
aggregate all bought_something (
    count(activity_id) as total_large_purchases_after
    -- typecasting to non-string types (e.g. int) require including a nullif clause and an explicit typecast
    -- initiate each filter criteria with a filter keyword (not and)
    filter nullif({{dbt_aql.json_extract('{feature_json}', 'total_sales')}}, '')::int > 100
    filter nullif({{dbt_aql.json_extract('{feature_json}', 'total_items_purchased')}}, '')::int > 3
)
```

## **Adding Custom Aggregation Functions**
Placeholder. Documentation coming soon.

## **Overriding Default Column Alias**
Placeholder. Documentation coming soon.

## **Overriding Default Model Prefix Removal**
Placeholder. Documentation coming soon.

## **Adding Custom Columns to Activity Schema**
Placeholder. Documentation coming soon.

## **Overriding the `listagg` Aggregation Function Default Delimiter**
Placeholder. Documentation coming soon.

# Contributing
For developers who would like to contribute to this project, follow the instructions below to set up your development environment.

## Setup
### General Setup
1. Fork the repository and clone the forked version
2. Install poetry ([official installer](https://python-poetry.org/docs/#installing-with-the-official-installer) - version 1.6.1 is recommended as that is what is used in CI)
3. Set the repo as the working directory
4. Run `poetry init` to install dependencies
5. Activate a virtual environment if not activated by default from previous step (`poetry shell`)
6. Set the working directory to `dbt-aql/integration_tests` and run `dbt deps`

### Postgres Setup
1. Install Postgres (`brew install postgresql@15` or similar version for MacOS/Homebrew, or [download the Postgres App](https://postgresapp.com/downloads.html))
2. Start Postgres on port 5432 (should be default - `brew services start postgresql@15` if using Homebrew)
3. Create a database called `dbt_aql_integration_tests`

### Bigquery Setup
1. Create a GCP account if you don't have one already
2. Create a Bigquery project if you don't have one already
3. Create a Bigquery service account if you don't have one already
4. Obtain a JSON keyfile for the service account and save it at the path `dbt-aql/integration_tests/gcp_keyfile.json`
5. Add the environment variable `GCP_KEYFILE_PATH=./gcp_keyfile.json`

## Testing
1. After making contributions, add dbt models and corresponding dbt tests in the `integration_tests` subdirectory that validate the new functionality works as expected. Add tests to capture edge cases to the best of your ability. In general, for each test case, the following artifacts need to be created:
    * a dbt model
    * a dbt seed csv file that contains the expected output for the dbt model
    * a `dbt_utils.equality` test comparing the model and the csv file
2. To validate that everything works, set `integration_tests` as the working directory, and run `dbt build --target <target_name>`, where `<target_name>` represents one of the targets defined in `integration_tests/profiles.yml`, and each target corresponds to a different database type. Valid options are:
    * `duckdb` (default)
    * `bigquery`
    * `postgres`
3. Be sure to follow the database-specific setup instructions above to test appropriately
