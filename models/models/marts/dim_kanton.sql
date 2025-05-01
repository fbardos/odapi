with src as (
    select *
    from {{ ref('intm_swissboundaries_kanton') }}
)
-- When a Gemeinde stops to exist, no additional rows
-- are generated from a given snapshot_date onwards.
, max_snapshot_per_kanton as (
    select distinct
        kanton_bfs_id
        , max(snapshot_date) as max_snapshot_date
    from src
    group by kanton_bfs_id
)
, add_row_for_latest as (
    select distinct
        'latest' as snapshot_code
        , bound.snapshot_date as snapshot_date
        , bound.kanton_bfs_id
        , bound.kanton_name
        , bound.icc
        , bound.geometry
    from src bound
        join max_snapshot_per_kanton maxd on
            bound.kanton_bfs_id = maxd.kanton_bfs_id
            and bound.snapshot_date = maxd.max_snapshot_date

)
, union_tables as (
    select
        to_char(bound.snapshot_date, 'YYYY-MM-DD') as snapshot_code
        , bound.snapshot_date
        , bound.kanton_bfs_id
        , bound.kanton_name
        , bound.icc
        , bound.geometry
    from src bound
    UNION ALL
    select *
    from add_row_for_latest
)
select
    snapshot_code
    , snapshot_date
    , kanton_bfs_id::SMALLINT
    , kanton_name
    , icc
    , geometry
    , EXTRACT(YEAR FROM src.snapshot_date) as snapshot_year
    , ST_Transform(src.geometry, 4326) as geom_border
    , ST_Transform(
        ST_SetSRID(
            ST_CoverageSimplify(src.geometry, 50, TRUE) OVER (PARTITION BY src.snapshot_code),
            2056
        ),
        4326
    ) as geom_border_simple_50m
    , ST_Transform(
        ST_SetSRID(
            ST_CoverageSimplify(src.geometry, 100, TRUE) OVER (PARTITION BY src.snapshot_code),
            2056
        ),
        4326
    ) as geom_border_simple_100m
    , ST_Transform(
        ST_SetSRID(
            ST_CoverageSimplify(src.geometry, 500, TRUE) OVER (PARTITION BY src.snapshot_code),
            2056
        ),
        4326
    ) as geom_border_simple_500m
from union_tables src
