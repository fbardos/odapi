with time_window_yearly as (
    select date_trunc('day', dd)::DATE as snapshot_date
    from generate_series('1850-01-01'::TIMESTAMP, now()::TIMESTAMP, '1 year'::INTERVAL) dd
)
, src_old as (
    select
        1 as src_order
        , t.snapshot_date
        , bound."GDENR" as gemeinde_bfs_id
        , bound."GDEHISTID" as gemeinde_hist_bfs_id
        , bound."GDENAME" as gemeinde_name
        , bound."BEZNR" as bezirk_bfs_id
        , bound."KTNR" as kanton_bfs_id
        , ST_TRANSFORM(ST_SetSRID(bound.geometry, 4326), 2056) as geometry
    from {{ ref('stgn_swissboundaries_2015') }} bound
        join time_window_yearly t on
            t.snapshot_date between
                to_date(bound."VALID_FROM", 'YYYYMMDD')
                AND to_date(bound."VALID_UNTI", 'YYYYMMDD')
)
, src_current as (
    select distinct
        2 as src_order
        , to_date(_snapshot_year::TEXT, 'YYYY') as snapshot_date
        , bfs_nummer as gemeinde_bfs_id
        , hist_nr as gemeinde_hist_bfs_id
        , name as gemeinde_name
        , bezirksnummer as bezirk_bfs_id
        , kantonsnummer as kanton_bfs_id
        , ST_SetSRID(ST_Force2D(geometry), 2056) as geometry
    from {{ ref('stgn_swissboundaries') }}
)
, union_tbl as (
    select *
    from src_old
    UNION ALL
    select *
    from src_current
)
, make_sure_unique as (
    select distinct on (gemeinde_bfs_id, snapshot_date)
        snapshot_date
        , gemeinde_bfs_id
        , gemeinde_hist_bfs_id
        , gemeinde_name
        , bezirk_bfs_id
        , kanton_bfs_id
        , geometry
    from union_tbl
    order by gemeinde_bfs_id, snapshot_date asc, src_order desc
)

select *
from make_sure_unique