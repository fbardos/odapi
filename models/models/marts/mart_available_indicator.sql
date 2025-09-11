select
    api.geo_code
    , api.indicator_id
    , indi.indicator_name
    , indi.topic_1
    , indi.topic_2
    , indi.topic_3
    , indi.topic_4
    , indi.indicator_unit
    , indi.indicator_description
    , array_agg(distinct api.measure_code) as measure_codes
from {{ ref('full_mart_ogd_api') }} api
    join {{ ref('seed_indicator') }} indi on api.indicator_id = indi.indicator_id
group by
    api.indicator_id
    , api.geo_code
    , indi.indicator_name
    , indi.topic_1
    , indi.topic_2
    , indi.topic_3
    , indi.topic_4
    , indi.indicator_unit
    , indi.indicator_description
order by api.indicator_id


