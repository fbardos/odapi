select *
from {{ ref('intm_traffic_miv') }}
order by
    zeit_von desc
    , anlage_id
    , richtung
    , spur
