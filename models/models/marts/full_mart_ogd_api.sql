-- this model exists only to use the same
-- grans logic like for other dbt models
-- these grants are used by the downsrem API
select *
from {{ source('marts', 'mart_ogd_api') }}
