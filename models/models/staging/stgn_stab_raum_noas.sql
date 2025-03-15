-- depends_on: {{ ref('dim_gemeinde_latest') }}
{{ stgn_bfs_stat_tab(source('src', 'stat_tab_raum_noas')) }}

select
    year_from
    , year_to
    , REGEXP_REPLACE(noas04, '^(>>|-|<->|\.{4})', '', 'g') as noas04
    , indicator_value
    , geo_code
    , geo_value
    , geo_value_name
    , source
    , _surr_key
from final
