select
    gemeinde_bfs_nr                     ::UInt16        as gemeinde_bfs_nr
    , gemeinde                          ::String        as gemeinde
    , anlage_nr                         ::String        as anlage_nr
    , anlage_name                       ::String        as anlage_name
    , anlage_typ                        ::String        as anlage_typ
    , parseDateTimeBestEffort(zeit_von)                 as zeit_von
    , parseDateTimeBestEffort(zeit_bis)                 as zeit_bis
    , richtung                          ::String        as richtung
    , richtung_name                     ::String        as richtung_name
    , spur_nr                           ::UInt8         as spur_nr
    , lat                               ::Float32       as lat
    , lon                               ::Float32       as lon
    , anzahl                            ::UInt32        as anzahl
    , file_source                       ::String        as file_source
    , publisher                         ::String        as publisher
    , _record                           ::UInt32        as _record
    , _partition_datetime
from {{ source('src', 'traffic_bicycle_winti')}}
