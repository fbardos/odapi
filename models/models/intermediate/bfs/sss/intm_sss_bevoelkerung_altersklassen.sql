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
        , sss.age0_4
        , sss.age5_9
        , sss.age10_14
        , sss.age15_19
        , sss.age20_24
        , sss.age25_29
        , sss.age30_34
        , sss.age35_39
        , sss.age40_44
        , sss.age45_49
        , sss.age50_54
        , sss.age55_59
        , sss.age60_64
        , sss.age65_69
        , sss.age70_74
        , sss.age75_79
        , sss.age80_84
        , sss.age85_89
        , sss.age90_94
        , sss.age95_99
        , sss.age100_plus
    from {{ ref('snap_sss_bevoelkerung_altersklassen') }} sss
        -- ugly join
        left join {{ ref('intm_swissboundaries_gemeinde') }} gem on
            sss.gemeinde_name = gem.gemeinde_name
            AND sss.year = extract(YEAR from gem.snapshot_date)
)
, union_all as (
    {{ intm_sss_map_indicators('src') }}
)

select * from union_all
