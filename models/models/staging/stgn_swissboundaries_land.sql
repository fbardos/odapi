select *
from {{ source('src', 'bfs_swissboundaries_land') }}
