select * except (dbt_valid_from, dbt_valid_to)
from {{ ref('mart_traffic_miv_history') }}
where
    dbt_valid_to is null
