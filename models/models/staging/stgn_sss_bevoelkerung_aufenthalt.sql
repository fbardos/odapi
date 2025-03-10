select
    src.gemeinde_name
    , src.year
    , src.source
    {% set indicators = [
        'auslaender',
        'aufenthalt_b',
        'niedergelassen_c',
        'vorl_aufgenommen_f',
        'kurzaufenthalter_l',
        'asylsuchend_n',
        'schutzstatus_s',
        'diplomat',
        'andere',
    ] %}
    {{ stgn_sss_replace(indicators) }}
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_aufenthalt')}} src
