{% macro mart_partition() %}

    {%- set base_name = modules.re.sub('__.*$', '', model.name) -%}
    {%- set upstream_model_name = modules.re.sub('^mart_', 'intm_', base_name) ~ '__typecast' -%}

    with replace_with_ids as (
        select
            -- replacements of text to ids happen here
            -- so that it uses less storage and gets persisted as table
            indicator_id
            , geo_code
            , geo_value
            , knowledge_date_from
            , knowledge_date_to
            , period_type
            , period_code
            , period_ref_from
            , period_ref
            , group_1.group_id as group_1_id
            , group_value_1.group_value_id as group_value_1_id
            , case
                when coalesce(group_value_1.group_value_id, 1) = 1 then TRUE
                else FALSE
            end as _group_value_1_is_total
            , group_2.group_id as group_2_id
            , group_value_2.group_value_id as group_value_2_id
            , case
                when coalesce(group_value_2.group_value_id, 1) = 1 then TRUE
                else FALSE
            end as _group_value_2_is_total
            , group_3.group_id as group_3_id
            , group_value_3.group_value_id as group_value_3_id
            , case
                when coalesce(group_value_3.group_value_id, 1) = 1 then TRUE
                else FALSE
            end as _group_value_3_is_total
            , group_4.group_id as group_4_id
            , group_value_4.group_value_id as group_value_4_id
            , case
                when coalesce(group_value_4.group_value_id, 1) = 1 then TRUE
                else FALSE
            end as _group_value_4_is_total
            , measure_code
            , indicator_value_numeric::NUMERIC(32, 9)  -- this is a limit from COPY TO PARQUET
            , indicator_value_text
            , dim.id as source_id
        from {{ ref(upstream_model_name) }} src
            left join {{ ref('dim_source') }} dim on src.source = dim.source
            left join {{ ref('dim_group') }} group_1 on src.group_1_name = group_1.group_name
            left join {{ ref('dim_group') }} group_2 on src.group_2_name = group_2.group_name
            left join {{ ref('dim_group') }} group_3 on src.group_3_name = group_3.group_name
            left join {{ ref('dim_group') }} group_4 on src.group_4_name = group_4.group_name
            left join {{ ref('dim_group_value') }} group_value_1 on
                src.group_1_value = group_value_1.group_value_name
            left join {{ ref('dim_group_value') }} group_value_2 on
                src.group_2_value = group_value_2.group_value_name
            left join {{ ref('dim_group_value') }} group_value_3 on
                src.group_3_value = group_value_3.group_value_name
            left join {{ ref('dim_group_value') }} group_value_4 on
                src.group_4_value = group_value_4.group_value_name
    )

    select * from replace_with_ids

{% endmacro %}
