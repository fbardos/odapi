-- Copy of dbt_expectations.expect_table_columns_to_match_ordered_list,
-- because - unlike macros - tests cannot be referenced in other tests.
{% test odapi_intm_columns(model, transform='upper') %}
{% set column_list = [
    'indicator_id',
    'geo_code',
    'geo_value',
    'knowledge_date_from',
    'knowledge_date_to',
    'period_type',
    'period_code',
    'period_ref_from',
    'period_ref',
    'indicator_value_numeric',
    'indicator_value_text',
    'source',
] %}
{{ return(default__expect_table_columns_to_match_ordered_list(model, column_list, transform)) }}
{% endtest %}


--------------------------------------------------------------------------------
-- GLOBAL PK
--------------------------------------------------------------------------------
{% test odapi_intm_pk(model) %}
    {% set combination_of_columns = ['indicator_id', 'geo_code', 'geo_value', 'period_ref', 'knowledge_date_from'] %}
    {{ return(adapter.dispatch('test_unique_combination_of_columns', 'dbt_utils')(model, combination_of_columns, quote_columns=False)) }}
{% endtest %}


--------------------------------------------------------------------------------
-- expect at least 2000 municipalities in tables with municipalities
--------------------------------------------------------------------------------
{% test odapi_intm_unique_municipalities(model) %}
    {{ adapter.dispatch('test_expect_table_row_count_to_be_between', 'dbt_expectations') (
        model,
        min_value=2000,
        max_value=None,
        group_by=['knowledge_date_from', 'period_ref'],
        row_condition="geo_code = 'polg'",
        strictly=False,
    ) }}
{% endtest %}


--------------------------------------------------------------------------------
-- expect at least one row
--------------------------------------------------------------------------------
{% test odapi_intm_rowsmin(model) %}
	{{ adapter.dispatch('test_expect_table_row_count_to_be_between', 'dbt_expectations') (
        model,
        min_value=1,
        max_value=None,
        group_by=None,
        row_condition=None,
        strictly=False,
) }} 
{% endtest %}
