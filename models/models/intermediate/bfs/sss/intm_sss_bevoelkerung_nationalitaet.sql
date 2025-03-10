with src as (
    select
        'polg' as geo_code
        , gem.gemeinde_bfs_id as geo_value
        , sss.dbt_valid_from as knowledge_date_from
        , sss.dbt_valid_to as knowledge_date_to
        , 'duedate' as period_type
        , 'day' as period_code
        , NULL::DATE as period_ref_from
        , make_date(sss.year::INTEGER, 12, 31) as period_ref
        , NULL::TEXT as indicator_value_text
        , sss.source
        , sss.schweizer
        , sss.auslaender
        , sss.schweizer_geboren_schweiz
        , sss.schweizer_geboren_ausland
        , sss.auslaender_geboren_schweiz
        , sss.auslaender_geboren_ausland
    from {{ ref('snap_sss_bevoelkerung_nationalitaet') }} sss
        -- ugly join
        left join {{ ref('intm_swissboundaries_gemeinde') }} gem on
            sss.gemeinde_name = gem.gemeinde_name
            AND sss.year = extract(YEAR from gem.snapshot_date)
)
, union_all as (
    {{ intm_sss_map_indicators('src') }}
)

select * from union_all
