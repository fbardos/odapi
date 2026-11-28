select
    mart.*
from {{ ref('intm_env_tree') }} mart
order by
    mart.id
    , dbt_valid_from
    
