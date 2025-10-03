{% macro intm_measures(upstream_model) %}
    {% set measure_config = none %}

    {% if execute %}
        {% set upstream_cfg = intm_get_upstream_config() %}
        {% set measure_config = upstream_cfg.get('odapi', {}).get('measure', none) %}
    {% endif %}

    -- XXX: Add db index for also fast-filter measure_code
    -- XXX: Add final CTE for all INTM macros, otherwise, will not work.
    {% set upstream_model_name = model.name.split('__')[0] %}
    with measure_src as (
        select *
        from {{ ref(upstream_model_name)}}
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

    {% if measure_config is none %}
        , measure_final as (
                select *
                -- when no measure.base is set, then it has to be a textual indicator
                -- like intm_bfs_statatlas_whg_energiequelle
                -- with filled indicator_value_text
                -- But in practice, when measure config gets forgotten to be set,
                -- default should not be 'text' as default, because otherise missleading
                , NULL::TEXT as measure_code
                from measure_src
        )
    {% else %}

        -- If base is not == zahl, then first calc back to zahl
        -- if base == anteil, then calc to anzahl
        {% if measure_config.get('base', none) == 'zahl' %}
            , measure_zahl as (
                select * from measure_src
            )
        {% elif measure_config.get('base', none) in ['pro1000', 'pro100000'] %}
            , measure_zahl as (
                select
                    meas.indicator_id
                    , meas.geo_code
                    , meas.geo_value
                    , meas.knowledge_date_from
                    , meas.knowledge_date_to
                    , meas.period_type
                    , meas.period_code
                    , meas.period_ref_from
                    , meas.period_ref
                    , meas.group_1_name
                    , meas.group_1_value
                    , meas.group_2_name
                    , meas.group_2_value
                    , meas.group_3_name
                    , meas.group_3_value
                    , meas.group_4_name
                    , meas.group_4_value
                    , meas.indicator_value_numeric * (
                        bev.indicator_value_numeric /
                        {% if measure_config.get('base', none) == 'pro100' %}
                            100.0
                        {% elif measure_config.get('base', none) == 'pro1000' %}
                            1000.0
                        {% elif measure_config.get('base', none) == 'pro100000' %}
                            100000.0
                        {% endif %}
                    ) as indicator_value_numeric
                    , meas.indicator_value_text
                    , meas.source
                    , meas._etl_version
                from measure_src meas
                    left join bev_src bev on
                        meas.geo_code = bev.geo_code
                        and meas.geo_value = bev.geo_value
                        and
                            extract(year from meas.period_ref)
                            = extract(year from bev.period_ref)
            )
        {% endif %}

        {% if measure_config.get('calc', none) is not none %}
            , measure_final as (
                select
                    meas.indicator_id
                    , meas.geo_code
                    , meas.geo_value
                    , meas.knowledge_date_from
                    , meas.knowledge_date_to
                    , meas.period_type
                    , meas.period_code
                    , meas.period_ref_from
                    , meas.period_ref
                    , meas.group_1_name
                    , meas.group_1_value
                    , meas.group_2_name
                    , meas.group_2_value
                    , meas.group_3_name
                    , meas.group_3_value
                    , meas.group_4_name
                    , meas.group_4_value
                    , meas.indicator_value_numeric
                    , meas.indicator_value_text
                    , meas.source
                    , meas._etl_version
                    , 'zahl' as measure_code
                from measure_zahl meas

                {% set _iter_without_zahl = measure_config.get('calc', []) + [measure_config.get('base', none)] %}
                {% set _iter_without_zahl = _iter_without_zahl | reject("equalto", 'zahl') | list %}
                {% for calc in _iter_without_zahl %}
                    UNION ALL
                    select
                        meas.indicator_id
                        , meas.geo_code
                        , meas.geo_value
                        , meas.knowledge_date_from
                        , meas.knowledge_date_to
                        , meas.period_type
                        , meas.period_code
                        , meas.period_ref_from
                        , meas.period_ref
                        , meas.group_1_name
                        , meas.group_1_value
                        , meas.group_2_name
                        , meas.group_2_value
                        , meas.group_3_name
                        , meas.group_3_value
                        , meas.group_4_name
                        , meas.group_4_value
                        , meas.indicator_value_numeric / (
                            bev.indicator_value_numeric /
                            {% if calc == 'pro100' %}
                                100.0
                            {% elif calc == 'pro1000' %}
                                1000.0
                            {% elif calc == 'pro100000' %}
                                100000.0
                            {% endif %}
                        ) as indicator_value_numeric
                        , meas.indicator_value_text
                        , meas.source
                        , meas._etl_version
                        , '{{ calc }}' as measure_code
                    from measure_zahl meas
                        left join bev_src bev on
                            meas.geo_code = bev.geo_code
                            and meas.geo_value = bev.geo_value
                            and
                                extract(year from meas.period_ref)
                                = extract(year from bev.period_ref)
                {% endfor %}
            )
        {% else %}
            , measure_final as (
                select
                    *
                    , '{{ measure_config.get("base", none) }}' as measure_code
                from measure_zahl
            )
        {% endif %}
    {% endif %}

    select
        meas.indicator_id::SMALLINT
        , meas.geo_code::CHAR(4)
        , meas.geo_value::SMALLINT
        , meas.knowledge_date_from::TIMESTAMP WITHOUT TIME ZONE
        , meas.knowledge_date_to::TIMESTAMP WITHOUT TIME ZONE
        , meas.period_type::TEXT
        , meas.period_code::TEXT
        , meas.period_ref_from::DATE
        , meas.period_ref::DATE
        , meas.group_1_name::TEXT
        , meas.group_1_value::TEXT
        , meas.group_2_name::TEXT
        , meas.group_2_value::TEXT
        , meas.group_3_name::TEXT
        , meas.group_3_value::TEXT
        , meas.group_4_name::TEXT
        , meas.group_4_value::TEXT
        , meas.indicator_value_numeric::NUMERIC
        , meas.indicator_value_text::TEXT
        , meas.source::TEXT
        , meas._etl_version::SMALLINT
        , meas.measure_code::TEXT
    from measure_final meas

{% endmacro %}
