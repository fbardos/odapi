select distinct on (bezirk_bfs_id)
    *
from {{ ref('dim_bezirk_g1') }}
order by
    bezirk_bfs_id
    , stichtag desc
    
