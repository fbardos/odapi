with src as (
    select
        case
            when indicator = 'AnzahlSteuerpflichtige' then 111
            when indicator = 'AnzahlSteuerpflichtigeohneSteuer' then 112
            when indicator = 'DurchschnittReineinkommen' then 113
            when indicator = 'DurchschnittReinesAequivalenzeinkommen' then 114
            when indicator = 'DurchschnittSteuerbaresAequivalenzeinkommen' then 115
            when indicator = 'DurchschnittSteuerbaresEinkommen' then 116
            when indicator = 'DurchschnittSteuerbetrag' then 117
            when indicator = 'EinnahmenDBST' then 118
            when indicator = 'GiniReineinkommen' then 119
            when indicator = 'GiniReineinkommenAeq' then 120
            when indicator = 'GiniSteuerbaresAequivalenzeinkommen' then 121
            when indicator = 'GiniSteuerbareseinkommen' then 122
            when indicator = 'GiniSteuerbetrag' then 123
            when indicator = 'MedianReineinkommen' then 124
            when indicator = 'MedianReinesAequivalenzeinkommen' then 125
            when indicator = 'MedianSteuerbaresAequivalenzeinkommen' then 126
            when indicator = 'MedianSteuerbaresEinkommen' then 127
            when indicator = 'MedianSteuerbetrag' then 128
            -- maybe extend later
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
        , indicator_value_numeric
        , NULL::TEXT as indicator_value_text
        , 'Eidgenössische Steuerverwaltung ESTV' as source
    from {{ ref('stgn_estv_db_np') }}
    where
        zivilstand = 'T'  -- Maybe extend later
        and geo_code = 'polg'  -- Maybe extend later
        and belastung_dbst = 'Steuerpflichtige mit und ohne einer Belastung durch die direkte Bundessteuer'  -- Maybe extend later
)
select *
from src
where indicator_id is not NULL
