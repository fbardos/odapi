-- _dataset and _resource must be merged
-- column publisher (which is needed for the CKAN JOIN from opendata.swiss)
-- needs the identifier for the publishers

with src_dataset as (
    -- this is important, will otherwise join with every DATE+DATENSATZ_ID later
    select distinct on (datensatz_id)
        datensatz_id
        , publisher
    from {{ ref('snap_ogd_usage_zh_dataset') }}
    order by
        datensatz_id
        , datum desc
)

, src_resource as (
    select *
    from {{ ref('snap_ogd_usage_zh_resource') }}
)

, union_src as (
    select
        res.*
        , ds.publisher as ckan_publisher
    from src_resource res
        left join src_dataset ds on
            res.datensatz_id = ds.datensatz_id
)

select * from union_src
