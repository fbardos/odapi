with time_window_yearly as (
    select date_trunc('day', dd)::DATE as snapshot_date
    from generate_series(
        '1850-01-01'::TIMESTAMP
        , now()::TIMESTAMP
        , '1 year'::INTERVAL
    ) as generated_dates(dd)
)
, src_old as (
    select
        1 as src_order
        , t.snapshot_date
        , bound."KTNR" as kanton_bfs_id
        , bound."KTKZ" as kanton_name
        , bound."CODE_ISO" as icc
        , ST_TRANSFORM(bound.geometry, 'EPSG:4326', 'EPSG:2056', true) as geometry
    from {{ ref('stgn_swissboundaries_2015') }} bound
        join time_window_yearly t on
            t.snapshot_date between
                strptime(bound."VALID_FROM"::VARCHAR, '%Y%m%d')::DATE
                AND strptime(bound."VALID_UNTI"::VARCHAR, '%Y%m%d')
)
, src_current as (
    select distinct
        2 as src_order
        , strptime(_snapshot_year::TEXT, '%Y') as snapshot_date
        , kantonsnummer as kanton_bfs_id
        , name as kanton_name
        , icc
        , ST_SetCRS(
            ST_Force2D(geometry),
            'EPSG:2056'
        ) as geometry
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
