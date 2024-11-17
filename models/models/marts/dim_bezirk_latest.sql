select *
from {{ ref('dim_bezirk') }}
where snapshot_code = 'latest'
