select
    gdename             ::String        as gemeinde
    , gdehistid         ::UInt32        as gemeinde_bfs_hist_id
    , gdenr             ::UInt16        as gemeinde_bfs_id
    , ktname            ::String        as kanton
    , ktnr              ::UInt8         as kanton_bfs_id
    , ktkz              ::String        as kanton_kuerzel
    , stichtag          ::Date32        as stichtag
    , readWKB(geometry) as geometry
from {{ source('src', 'bfs_gemeinde_g1') }}
