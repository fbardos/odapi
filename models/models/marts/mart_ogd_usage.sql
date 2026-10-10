select * except (dbt_valid_from, dbt_valid_to)
from {{ ref('mart_ogd_usage_history') }}
where
    dbt_valid_to is null
