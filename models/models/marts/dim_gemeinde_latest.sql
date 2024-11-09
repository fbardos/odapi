select
    *
from {{ ref('dim_gemeinde') }}
where snapshot_code = 'latest'
