select
    mart.*
from {{ ref('intm_traffic_miv') }} mart
order by
    mart.zeit_von desc
    , mart.anlage_id
    , mart.richtung
    , mart.spur
