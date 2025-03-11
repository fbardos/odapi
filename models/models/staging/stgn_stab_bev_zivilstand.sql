with src as (
    select
        year::INTEGER
        , bevtyp
        , geburtsort
        , geschlecht
        , zivilstand
        , indicator_value::NUMERIC
        , NULLIF(geo_code, 'nan') as geo_code
        , geo_value::INTEGER
        , geo_value_name
        , source
        , {{ dbt_utils.generate_surrogate_key(['year', 'bevtyp', 'geburtsort', 'geschlecht', 'zivilstand', 'geo_code', 'geo_value', 'geo_value_name']) }} as _surr_key
    from {{ source('src', 'stat_tab_bev_zivilstand') }}
)
select *
from src
where
    -- one of these columns has to be non-empty
    (
        geo_code is not NULL
        OR geo_value is not NULL
        OR geo_value_name is not NULL
    )
