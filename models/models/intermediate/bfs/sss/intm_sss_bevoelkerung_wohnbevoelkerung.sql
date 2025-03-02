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
        -- , staendig_total_yyyy
        , sss.staendig_total_vor_yyyy_minus_10   -- currently not needed
        , sss.staendig_veraenderung_10_jahre
        -- currently not needed (already from another source)
        , sss.staendig_bevoelkerungsdichte_yyyy
    from {{ ref('snap_sss_bevoelkerung_wohnbevoelkerung') }} sss
        -- ugly join
        left join {{ ref('intm_swissboundaries_gemeinde') }} gem on
            sss.gemeinde_name = gem.gemeinde_name
            AND sss.year = extract(YEAR from gem.snapshot_date)
)
, union_all as (
    select
        129 as indicator_id
        , geo_code
        , geo_value
        , knowledge_date_from
        , knowledge_date_to
        , period_type
        , period_code
        , period_ref_from
        , period_ref
        , staendig_veraenderung_10_jahre as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
)

select * from union_all
