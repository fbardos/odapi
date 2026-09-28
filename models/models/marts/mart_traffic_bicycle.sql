select
    {{ dbt_utils.star(from=ref('intm_traffic_bicycle'), except=['geometry']) }}
    -- geometry should not be nullable according to tests
    -- this will allow fastapi to export a geoparquet instead of just parquet
    , assumeNotNull(geometry) as geometry
    , geometry.1 as lon
    , geometry.2 as lat
from {{ ref('intm_traffic_bicycle') }}
order by
    zeit_von desc
    , standort_id
    , richtung
    , spur_id
    
