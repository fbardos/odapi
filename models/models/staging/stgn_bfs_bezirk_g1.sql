select
    bezname             ::String        as bezirk
    , bezhistid         ::UInt16        as bezirk_bfs_hist_id
    , beznr             ::UInt8         as bezirk_bfs_id
    , ktname            ::String        as kanton
    , ktnr              ::UInt8         as kanton_bfs_id
    , ktkz              ::String        as kanton_kuerzel
    , stichtag          ::Date32        as stichtag
    , readWKB(geometry) as geometry
from {{ source('src', 'bfs_bezirk_g1') }}
