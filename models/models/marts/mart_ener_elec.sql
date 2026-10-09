select * except (dbt_valid_from, dbt_valid_to)
from {{ ref('mart_ener_elec_history') }}
where
    dbt_valid_to is null
