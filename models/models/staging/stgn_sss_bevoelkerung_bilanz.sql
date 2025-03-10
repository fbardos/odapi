select
    src.gemeinde_name
    , src.year
    , src.source
    {% set indicators = [
        'wohnbevoelkerung_jahresanfang',
        'lebendgeburten',
        'todesfaelle',
        'geburtenueberschuss',
        'zuzug',
        'wegzug',
        'wanderungssaldo',
        'bev_gesamtbilanz',
        'bev_gesamtbilanz_prozent',
        'wohnbevoelkerung_jahresende',
        'lebendgeburten_pro_1000',
        'todesfaelle_pro_1000',
        'geburtenueberschuss_pro_1000',
        'zuzug_pro_1000',
        'wegzug_pro_1000',
        'wanderungssaldo_pro_1000',
        'bev_gesamtbilanz_pro_1000',
    ] %}
    {{ stgn_sss_replace(indicators) }}
    , {{ dbt_utils.generate_surrogate_key(['year', 'gemeinde_name']) }} as _surrogate_key
from {{ source('src', 'sss_bevoelkerung_bilanz')}} src
