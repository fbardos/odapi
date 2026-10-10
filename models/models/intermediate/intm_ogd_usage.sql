with odch_meta as (
    select
        od.identifier
        , od.id
        , od.title__de
        , od.url
        , groupUniqArray(odg.display_name__de) as topics
        , groupUniqArray(odt.value) as keywords
    from {{ source('src', 'bfs_odch_meta___meta') }} od
        left join {{ source('src', 'bfs_odch_meta___meta__groups') }} odg on
            od._dlt_id = odg._dlt_root_id
        left join {{ source('src', 'bfs_odch_meta___meta__keywords__de') }} odt on
            od._dlt_id = odt._dlt_root_id
    where
        od._dlt_valid_to is null -- FIX: no proper historization, fix in future
    group by all
)

, src_zueri_25 as (
    select
        src.date                    ::DATE              as date
        , od.id                     ::Nullable(String)  as dataset_id
        , src.resource_id           ::Nullable(String)  as resource_id
        , sum(src.hit_count)                            as downloads
        , sum(NULL::Nullable(UInt32))                   as visits
        , od.topics                                     as topics
        , od.keywords                                   as keywords
        , od.title__de                                  as datensatz
        , src.file_source
        , src.publisher
        , src.dbt_valid_from
        , src.dbt_valid_to
    from {{ ref('snap_ogd_usage_zueri_25') }} src
        left join odch_meta od on
            src.dataset_name = extract(od.url, '[^/]+$')
    group by all
)

, src_zueri_26 as (
    select
        src.date                    ::DATE              as date
        , od.id                     ::Nullable(String)  as dataset_id
        , src.resource_id           ::Nullable(String)  as resource_id
        , sum(src.hit_count)                            as downloads
        , sum(NULL::Nullable(UInt32))                   as visits
        , od.topics                                     as topics
        , od.keywords                                   as keywords
        , od.title__de                                  as datensatz
        , src.file_source
        , src.publisher
        , src.dbt_valid_from
        , src.dbt_valid_to
    from {{ ref('snap_ogd_usage_zueri_26') }} src
        left join odch_meta od on
            src.dataset_name = extract(od.url, '[^/]+$')
    group by all
)

, src_zh_resource as (
    select
        src.datum                   ::DATE              as date
        , src.datensatz_id          ::Nullable(String)  as dataset_id
        , src.ressourcen_id         ::Nullable(String)  as resource_id
        , sum(src.anzahl_downloads)                     as downloads
        , sum(src.anzahl_besuchende)                    as visits
        , od.topics                                     as topics
        , od.keywords                                   as keywords
        , od.title__de                                  as datensatz
        , src.file_source
        , src.publisher
        , src.dbt_valid_from
        , src.dbt_valid_to
    from {{ ref('intm_ogd_usage_zh') }} src
        left join odch_meta od on
            -- FIX: Concat will be wrong for other publishers
            concat(src.datensatz_id, '@', src.ckan_publisher) = od.identifier
    group by all
)

, src_bl as (
    select
        src.date                    ::DATE              as date
        , src.dataset_id            ::Nullable(String)  as dataset_id
        , NULL                      ::Nullable(String)  as resource_id
        , sum(src.interactions)                         as downloads
        , sum(src.visitors)                             as visits
        , od.topics                                     as topics
        , od.keywords                                   as keywords
        , od.title__de                                  as datensatz
        , src.file_source
        , src.publisher
        , src.dbt_valid_from
        , src.dbt_valid_to
    from {{ ref('snap_ogd_usage_bl') }} src
        left join odch_meta od on
            concat(src.dataset_id, '@kanton-basel-landschaft') = od.identifier
    group by all
)


, union_sources as (
    select * from src_zueri_25
    UNION ALL
    select * from src_zueri_26
    UNION ALL
    select * from src_zh_resource
    UNION ALL
    select * from src_bl
)

select *
from union_sources
