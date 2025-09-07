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
    , 1::SMALLINT as _etl_version
    , 'zahl'::TEXT as measure_code
from union_all meas
