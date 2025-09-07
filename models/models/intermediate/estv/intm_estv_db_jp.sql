with src as (
    select
        case
            when indicator = 'Anzahl' then 106
            when indicator = 'Eigenkapital' then 107
            when indicator = 'Reingewinn' then 108
            when indicator = 'Steuer' then 109
            when indicator = 'DurchschnittProzentAbzugBeteiligung' then 110
            else NULL::INTEGER
        end as indicator_id
        , geo_code::TEXT
        , geo_value::INTEGER
        , make_date(2025, 1, 23)::TIMESTAMP as knowledge_date_from  -- Currently harcoded, make dynamic later.
        , NULL::TIMESTAMP as knowledge_date_to
        , 'duedate' as period_type
        , 'year' as period_code
        , NULL::DATE as period_ref_from
        , period_ref
        , NULL::TEXT as group_1_name
        , NULL::TEXT as group_1_value
        , NULL::TEXT as group_2_name
        , NULL::TEXT as group_2_value
        , NULL::TEXT as group_3_name
        , NULL::TEXT as group_3_value
        , NULL::TEXT as group_4_name
        , NULL::TEXT as group_4_value
        , indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , 'Eidgenössische Steuerverwaltung ESTV' as source
        , 1 as _etl_version
        , 'zahl' as measure_code
    from {{ ref('stgn_estv_db_jp') }}
    where
        quartile is NULL  -- Maybe change later
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
from src meas
where indicator_id is not NULL
