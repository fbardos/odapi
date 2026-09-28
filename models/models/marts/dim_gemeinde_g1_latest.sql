select distinct on (gemeinde_bfs_id)
    *
from {{ ref('dim_gemeinde_g1') }}
order by
    gemeinde_bfs_id
    , stichtag desc
