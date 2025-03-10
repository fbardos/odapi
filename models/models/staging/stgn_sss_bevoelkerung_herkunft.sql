select
    src.gemeinde_name
    , src.year
    , src.auslaender
    , src.auslaender_anteil
    {% set herkuenfte = [
        'eu_efta_total',
        'deutschland',
        'frankreich',
        'italien',
        'oesterreich',
        'spanien',
        'portugal',
        'uebrige_eu_total',
        'serbien',
        'tuerkei',
        'nordmazedonien',
        'russland',
        'asien_total',
        'sri_lanka',
        'indien',
        'china',
        'afrika',
        'nord_sued_amerika',
        'australasien',
        'andere_laender',
    ] %}
    {% for herkunft in herkuenfte %}
        , case 
            when src.{{ herkunft }}::TEXT in ('...', '…') then 0
            else src.{{ herkunft }}::INTEGER
        end as {{ herkunft }}
    {% endfor %}
    , src.source
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_herkunft')}} src
