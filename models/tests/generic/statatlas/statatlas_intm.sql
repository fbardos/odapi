-- Currently, for one geo_code, geo_value, there can be multiple period_ref
-- AND multiple SCD-Type2 validities.
-- Indicator should already be selected.
{% test statatlas_intm_pk(model) %}
    {% set combination_of_columns = ['geo_code', 'geo_value', 'period_ref', 'knowledge_date_from', 'measure_code'] %}
    {{ return(adapter.dispatch('test_unique_combination_of_columns', 'dbt_utils')(model, combination_of_columns, quote_columns=False)) }}
{% endtest %}
