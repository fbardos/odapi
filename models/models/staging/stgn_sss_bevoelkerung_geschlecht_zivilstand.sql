select
    src.gemeinde_name
    , src.year
    , src.source
    {% set indicators = [
        'frauen'
        , 'maenner'
        , 'ledig'
        , 'verheiratet_eingetragen_partnerschaft'
        , 'verwitwet'
        , 'geschieden'
    ] %}
    {{ stgn_sss_replace(indicators) }}
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_geschlecht_zivilstand')}} src
