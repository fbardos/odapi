select
    mart.*
    , coalesce(dkant.geometry, dgem.geometry) as geometry
from {{ ref('intm_ener_elec') }} mart
    left join {{ ref('dim_kanton_g1_latest') }} dkant on
        mart.geom_level = 'canton'
        and mart.geom_id = dkant.kanton_bfs_id
    left join {{ ref('dim_gemeinde_g1_latest') }} dgem on
        mart.geom_level = 'municipality'
        and mart.geom_id = dgem.gemeinde_bfs_id
order by
    mart.zeitpunkt desc
    , mart.geom_level
    , mart.geom_id
    
