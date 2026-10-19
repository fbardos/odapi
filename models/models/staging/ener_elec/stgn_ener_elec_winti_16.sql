select
    parseDateTimeBestEffort(zeitpunkt)            as zeitpunkt
    , bruttolastgang_kwh::Float32                       as bruttolastgang_kwh
    , file_source                       ::String        as file_source
    , publisher                         ::String        as publisher
    , geom_level                        ::String        as geom_level
    , geom_id                           ::UInt16        as geom_id
    , _record                           ::UInt32        as _record
    , _partition_datetime
from {{ source('src', 'ener_elec_winti_16')}}
