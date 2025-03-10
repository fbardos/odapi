select
    src.gemeinde_name
    , src.year
    , src.source
    {% set indicators = [
        'schweizer',
        'auslaender',
        'schweizer_geboren_schweiz',
        'schweizer_geboren_ausland',
        'auslaender_geboren_schweiz',
        'auslaender_geboren_ausland',
    ] %}
    {{ stgn_sss_replace(indicators) }}
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_nationalitaet')}} src
