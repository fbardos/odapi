select
    gml_id                              ::String        as gml_id
    , id                                ::String        as id
    , gemeinde                          ::String        as gemeinde
    , bfs                               ::UInt16        as gemeinde_bfs_id
    , standort                          ::String        as standort
    , hausnummer                        ::String        as hausnummer
    , eigentuemer                       ::String        as eigentuemer
    , bnr                               ::UInt16        as bnr
    , typ                               ::String        as typ
    , pflanzenname_botanisch            ::String        as pflanzenname_botanisch
    , pflanzenname_deutsch              ::String        as pflanzenname_deutsch
    , hoehe                             ::UInt16        as hoehe
    , kronendurchmesser                 ::UInt8         as kronendurchmesser
    , stammumfang                       ::UInt16        as stammumfang
    , pflanzjahr                        ::UInt16        as pflanzjahr
    , schutzwuerdigkeit                 ::String        as schutzwuerdigkeit
    , if(
        isNull(geometry),
        null,
        readWKBPoint(assumeNotNull(geometry))
    ) as geometry
    , _record
    , _partition_datetime
    , file_source
    , publisher
from {{ source('src', 'env_tree_zg') }}
