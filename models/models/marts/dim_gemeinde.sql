with src as (
    select *
    from {{ ref('intm_swissboundaries') }}
)
-- When a Gemeinde stops to exist, no additional rows
-- are generated from a given snapshot_date onwards.
, max_snapshot_per_gemeinde as (
    select distinct
        gemeinde_bfs_id
        , max(snapshot_date) as max_snapshot_date
    from src
    group by gemeinde_bfs_id
)
, add_row_for_latest as (
    select distinct
        'latest' as snapshot_code
        , bound.snapshot_date as snapshot_date
        , bound.gemeinde_bfs_id
        , bound.gemeinde_hist_bfs_id
        , bound.gemeinde_name
        , bound.bezirk_bfs_id
        , bound.kanton_bfs_id
        , bound.geometry
    from src bound
        join max_snapshot_per_gemeinde maxd on
            bound.gemeinde_bfs_id = maxd.gemeinde_bfs_id
            and bound.snapshot_date = maxd.max_snapshot_date

)
, union_tables as (
    select
        to_char(bound.snapshot_date, 'YYYY-MM-DD') as snapshot_code
        , bound.snapshot_date
        , bound.gemeinde_bfs_id
        , bound.gemeinde_hist_bfs_id
        , bound.gemeinde_name
        , bound.bezirk_bfs_id
        , bound.kanton_bfs_id
        , bound.geometry
    from src bound
    UNION ALL
    select *
    from add_row_for_latest
)
select *
from union_tables
