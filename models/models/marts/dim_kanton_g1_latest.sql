select distinct on (kanton_bfs_id)
    *
from {{ ref('dim_kanton_g1') }}
order by
    kanton_bfs_id
    , stichtag desc
