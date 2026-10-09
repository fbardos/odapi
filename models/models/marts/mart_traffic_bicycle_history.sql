select * except (geometry)
    , assumeNotNull(geometry) as geometry
    , geometry.1 as lon
    , geometry.2 as lat
from {{ ref('intm_traffic_bicycle') }}
order by
    zeit_von desc
    , standort_id
    , richtung
    , spur_id
    
