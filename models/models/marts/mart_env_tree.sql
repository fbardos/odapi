select * except (dbt_valid_from, dbt_valid_to)
from {{ ref('mart_env_tree_history') }}
where
    dbt_valid_to is null
