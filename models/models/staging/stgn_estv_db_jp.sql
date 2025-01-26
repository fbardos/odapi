with src as (
    {{ dbt_utils.unpivot(
        ref('seed_export_jp_2025-01-23'),
        exclude=['Typ', 'GemeindeID', 'Gemeinde', 'Steuerperiode', 'Quartil'],
        remove=['Kanton', 'KantonID', 'Gemeinde'],
        field_name='indicator',
        value_name='indicator_value',
        quote_identifiers=True,
    ) }}
)
select
    case
        when "Typ" = 'Gemeinde' then 'polg'
        else NULL::TEXT
    end as geo_code
    , "GemeindeID" as geo_value
    , make_date("Steuerperiode"::INTEGER, 12, 31) as period_ref
    , "Quartil" as quartile
    , indicator
    , case
        when indicator_value = 'NA' then NULL::NUMERIC
        else indicator_value::NUMERIC
    end as indicator_value_numeric
from src
