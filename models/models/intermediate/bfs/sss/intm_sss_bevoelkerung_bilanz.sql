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
        , sss.wohnbevoelkerung_jahresanfang
        , sss.lebendgeburten
        , sss.todesfaelle
        , sss.geburtenueberschuss
        , sss.zuzug
        , sss.wegzug
        , sss.wanderungssaldo
        , sss.bev_gesamtbilanz
        , sss.bev_gesamtbilanz_prozent
        , sss.wohnbevoelkerung_jahresende
        , sss.lebendgeburten_pro_1000
        , sss.todesfaelle_pro_1000
        , sss.geburtenueberschuss_pro_1000
        , sss.zuzug_pro_1000
        , sss.wegzug_pro_1000
        , sss.wanderungssaldo_pro_1000
        , sss.bev_gesamtbilanz_pro_1000
        , sss.source
    from {{ ref('snap_sss_bevoelkerung_bilanz') }} sss
        -- ugly join
        left join {{ ref('intm_swissboundaries_gemeinde') }} gem on
            sss.gemeinde_name = gem.gemeinde_name
            AND sss.year = extract(YEAR from gem.snapshot_date)
)
, union_all as (
    {{ intm_sss_map_indicators('src') }}
)

select * from union_all
