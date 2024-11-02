select *
from {{ source('src', 'bfs_swissboundaries_2015') }}
