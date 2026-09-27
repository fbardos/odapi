-- TODO: Same bug as for BS, data on time savings
-- are duplicated, therefore a select distinct on is needed
select distinct on (fk_standort, datum)
    fk_standort                         ::String        as standort_id
    , parseDateTimeBestEffort(datum)                    as datum
    , velo_in                           ::UInt16        as velo_in
    , velo_out                          ::UInt16        as velo_out
    , fuss_in                           ::UInt16        as fuss_in
    , fuss_out                          ::UInt16        as fuss_out
    , lon                               ::Float32       as ost
    , lat                               ::Float32       as nord
    , file_source                       ::String        as file_source
    , publisher                         ::String        as publisher
    , _record                           ::UInt32        as _record
    , _partition_datetime
from {{ source('src', 'traffic_bicycle_zueri')}}
order by
    fk_standort
    , datum
    , _record asc
