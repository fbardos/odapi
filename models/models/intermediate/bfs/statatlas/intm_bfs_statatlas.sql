with src as (
    select
        stat.*
        , NULL::TEXT as file_source
        , coalesce(
            gem.geom_border
            , bez.geom_border
            , kant.geom_border
        ) as geom
    from {{ ref('snap_bfs_statatlas_v2') }} stat
        left join {{ ref('dim_gemeinde_latest') }} gem on
            stat.bfs_nivgeo like 'polg%'
            and stat.geo_value = gem.gemeinde_bfs_id
        left join {{ ref('dim_bezirk_latest') }} bez on
            stat.bfs_nivgeo like 'bezk%'
            and stat.geo_value = bez.bezirk_bfs_id
        left join {{ ref('dim_kanton_latest') }} kant on
            stat.bfs_nivgeo like 'kant%'
            and stat.geo_value = kant.kanton_bfs_id
)

, lat_lon as (
    select
        *
        , ST_Y(ST_Centroid(geom)) as lat
        , ST_X(ST_Centroid(geom)) as lon
    from src
)

select * from lat_lon
