-- Convention: one mart per source?
{% macro intm_measures(upstream_model) %}
    {% if execute %}
        {% set measure_config = config.require('odapi').get('measure', none) %}
    {% endif %}

    -- XXX: Add final CTE for all INTM macros, otherwise, will not work.
    with measure_src as (
        select *
        from final  -- final of macro intm_bfs_statatlas etc
    )
    , bev_src as (
        select
            geo_code
            , geo_value
            , period_ref
            , indicator_value_numeric
        from {{ ref('intm_bfs_statatlas_einwohner_staendig') }}
        where
            knowledge_date_to is NULL
    )

    {% if measure_config is not none %}
    
        -- If base is not == zahl, then first calc back to zahl
        -- if base == anteil, then calc to anzahl
        {% if measure_config.get('base', none) == 'zahl' %}
            , measure_zahl as (
                select * from measure_src
            )
        {% elif measure_config.get('base', none) in ['pro1000', 'pro100000'] %}
            , measure_zahl as (
                select
                    indicator_id
                    , geo_code
                    , geo_value
                    , knowledge_date_from
                    , knowledge_date_to
                    , period_type
                    , period_code
                    , period_ref_from
                    , period_ref
                    , group_1_name
                    , group_1_value
                    , group_2_name
                    , group_2_value
                    , group_3_name
                    , group_3_value
                    , group_4_name
                    , group_4_value
                    , indicator_value_numeric * (
                        bev_src.indicator_value_numeric /
                        {% if measure_config.get('base', none) == 'pro1000' %}
                            1000.0
                        {% elif measure_config.get('base', none) == 'pro100000' %}
                            100000.0
                        {% endif %}
                    ) as indicator_value_numeric
                    , indicator_value_text
                    , source
                    , _etl_version
                from measure_src
                    left join bev_src on
                        measure_src.geo_code = bev_src.geo_code
                        and measure_src.geo_value = bev_src.geo_value
                        and
                            extract(year from measure_src.period_ref)
                            = extract(year from bev_src.period_ref)
            )
        {% endif %}
        
        {% if measure_config.get('calc', none) is not none %}
            , measure_final as (
                {% for calc in measure_config.get('calc', []) %}
                    {% if loop.index0 > 0 %}
                        UNION ALL
                    {% else %}
                        select
                            indicator_id
                            , geo_code
                            , geo_value
                            , knowledge_date_from
                            , knowledge_date_to
                            , period_type
                            , period_code
                            , period_ref_from
                            , period_ref
                            , group_1_name
                            , group_1_value
                            , group_2_name
                            , group_2_value
                            , group_3_name
                            , group_3_value
                            , group_4_name
                            , group_4_value
                            , indicator_value_numeric / (
                                bev_src.indicator_value_numeric /
                                {% if calc == 'pro1000' %}
                                    1000.0
                                {% elif calc == 'pro100000' %}
                                    100000.0
                                {% endif %}
                            ) as indicator_value_numeric
                            , indicator_value_text
                            , source
                            , _etl_version
                            , {{ calc }} as measure_code
                        from measure_zahl
                            left join bev_src on
                                measure_src.geo_code = bev_src.geo_code
                                and measure_src.geo_value = bev_src.geo_value
                                and
                                    extract(year from measure_src.period_ref)
                                    = extract(year from bev_src.period_ref)
                    {% endif %}
                {% endfor %}
            )
        {% else %}
            , measure_final as (
                select
                    *
                    , {{ measure_config.get('base', 'zahl') }} as measure_code
                from measure_zahl
            )
        {% endif %}
    {% else %}
        , measure_final as (select * from final)
    {% endif %}

    select * from measure_final

{% endmacro %}
