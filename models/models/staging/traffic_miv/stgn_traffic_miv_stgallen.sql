select *
from {{ source('src', 'traffic_miv_stgallen') }}
