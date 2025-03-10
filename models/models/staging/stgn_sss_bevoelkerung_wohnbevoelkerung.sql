select
    src.gemeinde_name
    , src.year
    , src.source
    {% set indicators = [
        'staendig_total_yyyy'
        , 'staendig_total_vor_yyyy_minus_10'
        , 'staendig_veraenderung_10_jahre'
        , 'staendig_bevoelkerungsdichte_yyyy'
        , 'volkszaehlung_2000'
        , 'volkszaehlung_1990'
        , 'volkszaehlung_1980'
        , 'volkszaehlung_1970'
        , 'volkszaehlung_1930'
    ] %}
    {{ stgn_sss_replace(indicators) }}
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_wohnbevoelkerung')}} src
