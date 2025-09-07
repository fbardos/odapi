with src as (
    select
        'polg' as geo_code
        , gemeinde_bfs_id as geo_value
        , dbt_valid_from as knowledge_date_from
        , dbt_valid_to as knowledge_date_to
        , 'duedate' as period_type
        , 'day' as period_code
        , NULL::DATE as period_ref_from
        , dbt_valid_from as period_ref
        , NULL::TEXT as group_1_name
        , NULL::TEXT as group_1_value
        , NULL::TEXT as group_2_name
        , NULL::TEXT as group_2_value
        , NULL::TEXT as group_3_name
        , NULL::TEXT as group_3_value
        , NULL::TEXT as group_4_name
        , NULL::TEXT as group_4_value
        , NULL::TEXT as indicator_value_text
        , 'Bundesamt für Energie BFE' as source
        , anzahl_minergie
        , anzahl_minergie_eco
        , anzahl_minergie_a
        , anzahl_minergie_a_eco
        , anzahl_minergie_p
        , anzahl_minergie_p_eco
    from {{ ref('snap_bfe_minergie') }}
)
, union_all as (
    select
        95 as indicator_id
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
        , anzahl_minergie as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        96 as indicator_id
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
        , anzahl_minergie_eco as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        97 as indicator_id
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
        , anzahl_minergie_a as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        98 as indicator_id
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
        , anzahl_minergie_a_eco as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        99 as indicator_id
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
        , anzahl_minergie_p as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
    UNION ALL
    select
        100 as indicator_id
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
        , anzahl_minergie_p_eco as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
    from src
)

, final as (
    select
        indicator_id::SMALLINT
        , geo_code::CHAR(4)
        , geo_value::SMALLINT
        , knowledge_date_from::TIMESTAMP WITHOUT TIME ZONE
        , knowledge_date_to::TIMESTAMP WITHOUT TIME ZONE
        , period_type
        , period_code
        , period_ref_from::DATE
        , period_ref::DATE
        , group_1_name
        , group_1_value
        , group_2_name
        , group_2_value
        , group_3_name
        , group_3_value
        , group_4_name
        , group_4_value
        , indicator_value_numeric::NUMERIC as indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , source
        , 1::SMALLINT as _etl_version
        , 'zahl' as measure_code
    from union_all
)


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
from final meas
