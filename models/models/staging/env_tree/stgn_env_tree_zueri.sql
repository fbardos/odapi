select
    baumnummer                          ::String        as baumnummer
    , objectid                          ::UInt32        as objectid
    , gml_id                            ::String        as gml_id
    , poi_id                            ::String        as poi_id
    , kategorie                         ::String        as kategorie
    , quartier                          ::String        as quartier
    , strasse                           ::String        as strasse
    , baumgattunglat                    ::String        as baumgattunglat
    , baumartlat                        ::String        as baumartlat
    , baumnamelat                       ::String        as baumnamelat
    , baumnamedeu                       ::String        as baumnamedeu
    , status                            ::String        as status
    , baumtyp                           ::UInt8         as baumtyp
    , baumtyptext                       ::String        as baumtyptext
    , kronendurchmesser                 ::UInt8         as kronendurchmesser
    , pflanzjahr                        ::UInt16        as pflanzjahr
    , genauigkeit                       ::String        as genauigkeit
    , if(
        isNull(geometry),
        null,
        readWKBPoint(assumeNotNull(geometry))
    ) as geometry
    , _record
    , _partition_datetime
    , file_source
    , publisher
from {{ source('src', 'env_tree_zueri') }}
