with src as (
    select
        timestamp_interval_start                            as zeitpunkt
        , parseDateTimeBestEffort(timestamp_interval_start_text) as zeitpunkt_utc
        , stromverbrauch_kwh::Float32                       as bruttolastgang_kwh
        , grundversorgte_kunden_kwh::Float32                as grundversorgte_kunden_kwh
        , freie_kunden_kwh::Float32                         as freie_kunden_kwh
        , year::UInt16                                      as jahr
        , month::UInt8                                      as monat
        , day::UInt8                                        as tag
        , weekday::UInt8                                    as wochentag
        , dayofyear::UInt16                                 as jahrestag
        , quarter::UInt8                                    as quartal
        , weekofyear::UInt8                                 as jahreswoche
        , file_source                       ::String        as file_source
        , publisher                         ::String        as publisher
        , geom_level                        ::String        as geom_level
        , geom_id                           ::UInt16        as geom_id
        , _record                           ::UInt32        as _record
        , _partition_datetime
    from {{ source('src', 'ener_elec_bs')}}
)

-- FIX: Currently getting duplicates on PK zeitpunkt_utc,
-- so SELECT DISTINCT ON is needed
select distinct on (zeitpunkt_utc)
    *
from src
order by
    zeitpunkt_utc
    , _record asc
