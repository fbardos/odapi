with src as (
    select
        duedate as stichtag
        , gem_no as gemeinde_bfs_id
        , name as gemeinde_name
        , zwg_3150 as wohnungen
        , zwg_3010 as erstwohnungen
        , zwg_3100 as erstwohnung_gleichgestellt
        , zwg_3110 as anteil_erstwohnung
        , zwg_3120 as anteil_zweitwohnung
        , zwg_3200 as verfahren_old
        , verfahren as verfahren_new
        , status
        , geometry
    from {{ source('src', 'swisstopo_zweitwohnungen') }}
)
select
    src.*
    , {{ dbt_utils.generate_surrogate_key(['stichtag', 'gemeinde_bfs_id']) }} as _surrogate_key
from src
