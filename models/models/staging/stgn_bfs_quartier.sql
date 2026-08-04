select
    "GMDEQNR"::INT as gemeindequartier_bfs_id
    , "NR"::SMALLINT as quartier_bfs_id
    , "NAME"::TEXT as quartier
    , "GMDE"::SMALLINT as gemeinde_bfs_id
    , "FLAECHE"::INT as flaeche_ha
    , geometry
from {{ source('src', 'bfs_quartier') }}
