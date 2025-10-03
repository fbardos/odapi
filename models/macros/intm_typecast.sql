{% macro intm_typecast(upstream_model) %}
    
    {% set upstream_model_name = model.name.split('__')[0] ~ '__measure' %}

    select 
        intm.indicator_id::SMALLINT
        , intm.geo_code::CHAR(4)
        , intm.geo_value::SMALLINT
        , intm.knowledge_date_from::TIMESTAMP WITHOUT TIME ZONE
        , intm.knowledge_date_to::TIMESTAMP WITHOUT TIME ZONE
        , intm.period_type::TEXT
        , intm.period_code::TEXT
        , intm.period_ref_from::DATE
        , intm.period_ref::DATE
        , intm.group_1_name::TEXT
        , intm.group_1_value::TEXT
        , intm.group_2_name::TEXT
        , intm.group_2_value::TEXT
        , intm.group_3_name::TEXT
        , intm.group_3_value::TEXT
        , intm.group_4_name::TEXT
        , intm.group_4_value::TEXT
        , intm.indicator_value_numeric::NUMERIC
        , intm.indicator_value_text::TEXT
        , intm.source::TEXT
        , intm._etl_version::SMALLINT
        , intm.measure_code::TEXT
    from {{ ref(upstream_model_name) }} intm

{% endmacro %}
