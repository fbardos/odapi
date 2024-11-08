select
    *
    , ST_AsText(geometry, 0) as geometry_wkt
from {{ ref('dim_kanton') }}
where snapshot_code = 'latest'
