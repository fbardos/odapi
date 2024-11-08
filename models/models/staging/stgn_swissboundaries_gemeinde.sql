select *
from {{ source('src', 'bfs_swissboundaries_gemeinde') }}
