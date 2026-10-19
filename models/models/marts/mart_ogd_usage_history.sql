select *
from {{ ref('intm_ogd_usage') }} mart
order by
    mart.date desc
    , mart.dataset_id
    , mart.resource_id
    , mart.dbt_valid_from
