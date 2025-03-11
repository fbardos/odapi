with src as (
    select
        year::INTEGER
        , nationalitaet_a
        , nationalitaet_b
        , indicator_value::NUMERIC
        , NULLIF(geo_code, 'nan') as geo_code
        , geo_value::INTEGER
        , geo_value_name
        , source
        , {{ dbt_utils.generate_surrogate_key(['year', 'nationalitaet_a', 'nationalitaet_b', 'geo_code', 'geo_value', 'geo_value_name']) }} as _surr_key
    from {{ source('src', 'stat_tab_bev_heirat') }}
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
