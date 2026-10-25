-- TODO: Duplicates along zeit_von + anlage_name are possible
select
    toTimeZone(
        toDateTime64(datum, 3, 'Europe/Zurich'),
        'UTC'
    )                                                   as zeit_von
    , bezeichnung                       ::String        as anlage_name
    , any(readWKBPoint(assumeNotNull(standort)))        as geometry
    , sum(anzahl_velos::UInt16)         ::UInt16        as anzahl
    , any(file_source)                  ::String        as file_source
    , any(publisher)                    ::String        as publisher
    , array_agg(_record::UInt32)                        as _record
    , any(_partition_datetime)                          as _partition_datetime
from {{ source('src', 'traffic_bicycle_stgallen')}}
group by all
