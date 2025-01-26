with src as (
    {{ dbt_utils.unpivot(
        ref('seed_export_np_2025-01-23'),
        exclude=['BelastungDBST', 'GemeindeID', 'Steuerperiode', 'Zivilstand'],
        remove=['Kanton', 'KantonID', 'Gemeindename', 'ZivilstandBez'],
        field_name='indicator',
        value_name='indicator_value',
        quote_identifiers=True,
    ) }}
)
select
    "BelastungDBST" as belastung_dbst
    , case
        when "GemeindeID" = 'NA' then 'kant'
        else 'polg'
    end as geo_code
    , "GemeindeID" as geo_value
    , make_date("Steuerperiode"::INTEGER, 12, 31) as period_ref
    , "Zivilstand" as zivilstand
    , indicator
    , case
        when indicator_value = 'NA' then NULL::NUMERIC
        else indicator_value::NUMERIC
    end as indicator_value_numeric
from src
