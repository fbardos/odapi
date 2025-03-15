with src as (
    select
        'polg' as geo_code
        , gemeinde_bfs_id as geo_value
        , dbt_valid_from as knowledge_date_from
        , dbt_valid_to as knowledge_date_to
        , 'duedate' as period_type
        , 'day' as period_code
        , NULL::DATE as period_ref_from
        , stichtag as period_ref
        , NULL::TEXT as group_1_name
        , NULL::TEXT as group_1_value
        , NULL::TEXT as group_2_name
        , NULL::TEXT as group_2_value
        , NULL::TEXT as group_3_name
        , NULL::TEXT as group_3_value
        , NULL::TEXT as group_4_name
        , NULL::TEXT as group_4_value
        , NULL::TEXT as indicator_value_text
        , 'Wohnungsinventar - Bundesamt für Raumentwicklung' as source
        , wohnungen
        , erstwohnungen
        , erstwohnung_gleichgestellt
        , anteil_erstwohnung
        , anteil_zweitwohnung
    from {{ ref('snap_swisstopo_zweitwohnung') }}
)
, union_all as (
    select
        101 as indicator_id
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
        , anteil_erstwohnung as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        102 as indicator_id
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
        , anteil_zweitwohnung as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        103 as indicator_id
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
        , erstwohnungen as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        104 as indicator_id
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
        , erstwohnung_gleichgestellt as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        105 as indicator_id
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
        , wohnungen as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
)
select * from union_all
