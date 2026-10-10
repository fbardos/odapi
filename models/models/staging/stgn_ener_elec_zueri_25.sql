with src as (
    select
        parseDateTimeOrNull(zeitpunkt, '%Y-%m-%dT%H:%i', 'Europe/Zurich') as zeitpunkt
        , bruttolastgang::Float32                           as bruttolastgang_kwh
        , status::String                                    as status
        , file_source                       ::String        as file_source
        , publisher                         ::String        as publisher
        , geom_level                        ::String        as geom_level
        , geom_id                           ::UInt16        as geom_id
        , _record                           ::UInt32        as _record
        , _partition_datetime
    from {{ source('src', 'ener_elec_zueri_25')}}
    where
        -- one row contains NULL values
        zeitpunkt is not null
)

-- FIX: time savings duplicates (e.g. 2026-03-29)
-- Use a SELECT DISTINCT ON, but empasize improvement in the future
select distinct on (zeitpunkt)
    *
from src
order by
    zeitpunkt
    , _record
