with time_window_yearly as (
    select date_trunc('day', dd)::DATE as snapshot_date
    from generate_series('1850-01-01'::TIMESTAMP, now()::TIMESTAMP, '1 year'::INTERVAL) dd
)
, src_old as (
    select
        1 as src_order
        , t.snapshot_date
        , bound."KTNR" as kanton_bfs_id
        , bound."KTKZ" as kanton_name
        , bound."CODE_ISO" as icc
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
        , kantonsnummer as kanton_bfs_id
        , name as kanton_name
        , icc
        , ST_SetSRID(ST_Force2D(geometry), 2056) as geometry
    from {{ ref('stgn_swissboundaries_kanton') }}
)
, union_tbl as (
    select *
    from src_old
    UNION ALL
    select *
    from src_current
)
, make_sure_unique as (
    select distinct on (kanton_bfs_id, snapshot_date)
        snapshot_date
        , kanton_bfs_id
        , kanton_name
        , icc
        , geometry
    from union_tbl
    order by kanton_bfs_id, snapshot_date asc, src_order desc
)

select *
from make_sure_unique
