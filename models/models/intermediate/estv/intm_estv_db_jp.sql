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
        , geo_code
        , geo_value::INTEGER
        , make_date(2025, 1, 23) as knowledge_date_from  -- Currently harcoded, make dynamic later.
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
    from {{ ref('stgn_estv_db_jp') }}
    where
        quartile is NULL  -- Maybe change later
)
select *
from src
where indicator_id is not NULL
