-- duplicate tree on ID 25921, fix
-- TODO: report to authority
select distinct on (baumnummer)
    baumnummer                          ::UInt32        as baumnummer
    , baumtyp                           ::String        as baumtyp
    , baumart_d                         ::String        as baumart_d
    , trim(baumart_l)                   ::String        as baumart_l
    , baumhoehe                         ::Decimal       as baumhoehe
    , faellung                          ::Bool          as faellung
    , fid                               ::UInt32        as fid
    , groesse_darstellung               ::UInt8         as groesse_darstellung
    , kronendurchmesser                 ::UInt8         as kronendurchmesser
    , pflanzjahr                        ::UInt16        as pflanzjahr
    , z                                 ::Decimal       as z
    , if(
        isNull(geometry),
        null,
        readWKBPoint(assumeNotNull(geometry))
    ) as geometry
    , _record
    , _partition_datetime
    , file_source
    , publisher
from {{ source('src', 'env_tree_winti') }}
order by
    baumnummer
    , _record desc
