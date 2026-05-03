-- XXX: Currently not used, is refactored version of
-- intm_build_total in intm_bfs_stat_tab
-- Also, embed in bash script to create INTM models

{% macro intm_build_group_total(upstream_model) %}

    {% set upstream_model_name = model.name.split('__')[0] %}
    {% if execute %}
        {% set upstream_cfg = intm_get_upstream_config() %}
        {% set measure_config = upstream_cfg.get('odapi', {}).get('measure', none) %}
    {% endif %}

    -- XXX: Currently very slow, maybe find a better, cleaner solution for this.

    with src as (
        select
            intm.*
            -- TODO: not sure, if knowledge_date_from is correct, what if only
            -- some rows get updated?
            , {{ dbt_utils.generate_surrogate_key([
                'indicator_id',
                'geo_code',
                'geo_value',
                'knowledge_date_from',
                'period_ref',
            ]) }} as _sk_for_grouping
        from {{ ref(upstream_model_name) }} intm
    )

    , base_flattened as (
        select
            t._sk_for_grouping
            , outer_kv.key as attribute_name
            , inner_kv.key as member_code
            , inner_kv.value #>> '{}' as member_value
            , t.indicator_value_numeric
        from src t
        cross join lateral jsonb_each(t.grouping) as outer_kv(key, value)
        cross join lateral jsonb_each(outer_kv.value) as inner_kv(key, value)
    )

    , aggregated as (

        select
            case
                when grouping(_sk_for_grouping) = 1 then '-1'
                else _sk_for_grouping
            end as _sk_for_grouping
            , case
                when grouping(attribute_name) = 1 then '_ALL'
                else attribute_name
            end as attribute_name
            , case
                when grouping(member_code) = 1 then '_T'
                else member_code
            end as member_code
            , case
                when grouping(member_code) = 1 and grouping(_sk_for_grouping) = 1 then 'GRAND TOTAL'
                when grouping(member_code) = 1 then 'GROUP TOTAL'
                else member_value
            end as member_value
            -- XXX: There is a option in intm models: build_total_value.
            -- Add this functionality to respect this statement,
            -- instead of hardcoded sum()
            -- iterate per indicator (or maybe not needed because of _sk_for_grouping)
            , sum(indicator_value_numeric) as indicator_value_numeric
            , case
                when grouping(member_code) = 1 and grouping(_sk_for_grouping) = 0 then true
                else false
            end as is_group_total
            , case
                when grouping(_sk_for_grouping) = 1 then true
                else false
            end as is_grand_total
        from base_flattened
        group by grouping sets (
            (_sk_for_grouping, attribute_name, member_code, member_value),
            (_sk_for_grouping, attribute_name),
            ()
        )
    )

    , final as (
        select
            _sk_for_grouping,
            attribute_name,
            member_code,
            member_value,
            indicator_value_numeric,
            is_group_total,
            is_grand_total,
            {{ dbt_utils.generate_surrogate_key([
                "cast(_sk_for_grouping as " ~ dbt.type_string() ~ ")",
                "attribute_name",
                "member_code"
            ]) }} as business_key
        from aggregated

    )

    select *
    from final

    -- XXX: Continue from here: https://chatgpt.com/share/69e4ea7c-5f2c-838f-979b-62232989f9b0
    -- XXX: Steps: Build total per group, then build total over all groups
    -- XXX:: WORK IN PROGRESS

{% endmacro %}
