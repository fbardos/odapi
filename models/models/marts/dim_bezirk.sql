with src as (
    select *
    from {{ ref('intm_swissboundaries_bezirk') }}
)
-- When a Gemeinde stops to exist, no additional rows
-- are generated from a given snapshot_date onwards.
, max_snapshot_per_bezirk as (
    select distinct
        bezirk_bfs_id
        , max(snapshot_date) as max_snapshot_date
    from src
    group by bezirk_bfs_id
)
, add_row_for_latest as (
    select distinct
        'latest' as snapshot_code
        , bound.snapshot_date as snapshot_date
        , bound.bezirk_bfs_id
        , bound.bezirk_name
        , bound.kanton_bfs_id
        , bound.geometry
    from src bound
        join max_snapshot_per_bezirk maxd on
            bound.bezirk_bfs_id = maxd.bezirk_bfs_id
            and bound.snapshot_date = maxd.max_snapshot_date

)
, union_tables as (
    select
        to_char(bound.snapshot_date, 'YYYY-MM-DD') as snapshot_code
        , bound.snapshot_date
        , bound.bezirk_bfs_id
        , bound.bezirk_name
        , bound.kanton_bfs_id
        , bound.geometry
    from src bound
    UNION ALL
    select *
    from add_row_for_latest
)
select *
from union_tables
