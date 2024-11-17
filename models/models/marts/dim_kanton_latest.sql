select *
from {{ ref('dim_kanton') }}
where snapshot_code = 'latest'
