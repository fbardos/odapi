select
    ktname            ::String        as kanton
    , ktnr              ::UInt8         as kanton_bfs_id
    , ktkz              ::String        as kanton_kuerzel
    , stichtag          ::Date32        as stichtag
    , readWKB(geometry) as geometry
from {{ source('src', 'bfs_kanton_g1') }}
