select
    {{ dbt_utils.star(from=ref('intm_traffic_bicycle'), except=['zeit_von']) }}
    -- will otherwise not be tz-aware in csv export
    , formatDateTime(zeit_von, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS zeit_von
    , geometry.1 as lon
    , geometry.2 as lat
from {{ ref('intm_traffic_bicycle') }}
