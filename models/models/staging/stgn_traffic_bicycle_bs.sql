select
    zst_nr                              ::String        as zst_nr
    , zst_id                            ::UInt16        as zst_id
    , sitecode                          ::UInt16        as sitecode
    , sitename                          ::String        as sitename
    -- attention: multiple rows on timesaving
    , datetimefrom                      ::DateTime64    as datetimefrom
    , datetimeto                        ::DateTime64    as datetimeto
    , directionname                     ::String        as directionname
    , lanecode                          ::UInt8         as lanecode
    , lanename                          ::String        as lanename
    , valuesapproved                    ::Bool          as valuesapproved
    , valuesedited                      ::Bool          as valuesedited
    , traffictype                       ::String        as traffictype
    , total                             ::UInt16        as total
    , year                              ::UInt16        as year
    , month                             ::UInt8         as month
    , day                               ::UInt8         as day
    , weekday                           ::UInt8         as weekday
    , hourfrom                          ::UInt8         as hourfrom
    , parseDateTimeInJodaSyntax(date, 'dd.MM.yyyy')::Date as date
    , concat(timefrom, ':00')::Time                     as timefrom
    , concat(timeto, ':00')::Time                       as timeto
    , datetimefrom_utc
    , datetimeto_utc
    , dayofyear                         ::UInt16        as dayofyear
    , if(
        isNull(geo_point_2d),
        null,
        readWKBPoint(assumeNotNull(geo_point_2d))
    ) as geo_point_2d
    , file_source                       ::String        as file_source
    , publisher                         ::String        as publisher
    , _record                           ::UInt32        as _record
    , _partition_datetime
from {{ source('src', 'traffic_bicycle_bs')}}
