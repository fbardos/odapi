{% macro mart_partition() %}

    {%- set base_name = modules.re.sub('__.*$', '', model.name) -%}
    {%- set upstream_model_name = modules.re.sub('^mart_', 'intm_', base_name) ~ '__typecast' -%}

    with replace_with_ids as (
        -- passthrough (as no partition table needed anymore)
        select *
        from {{ ref(upstream_model_name) }} src
    )

    select * from replace_with_ids

{% endmacro %}
