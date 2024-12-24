select distinct
    api.geo_code
    , api.indicator_id
    , indi.indicator_name
    , indi.topic_1
    , indi.topic_2
    , indi.topic_3
    , indi.topic_4
    , indi.indicator_unit
    , indi.indicator_description
from {{ ref('mart_ogd_api') }} api
    join {{ ref('seed_indicator') }} indi on api.indicator_id = indi.indicator_id
order by api.indicator_id


