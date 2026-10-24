select
    gemeinde_code                       ::UInt16        as gemeinde_bfs_nr
    , gemeinde_name                     ::String        as gemeinde
    , messstelle_id                     ::String        as anlage_nr
    , NULL                              ::Nullable(String) as anlage_name
    , parseDateTimeBestEffort(zeit_von)                 as zeit_von
    , parseDateTimeBestEffort(zeit_bis)                 as zeit_bis
    , spur_richtung                     ::String        as spur_richtung
    , messstelle_laengengrad_wgs84      ::Float32       as lat
    , messstelle_breitengrad_wgs84      ::Float32       as lon
    , anzahl_velos                      ::UInt32        as anzahl
    , file_source                       ::String        as file_source
    , publisher                         ::String        as publisher
    , _record                           ::UInt32        as _record
    , _partition_datetime
from {{ source('src', 'traffic_bicycle_zh_21')}}
