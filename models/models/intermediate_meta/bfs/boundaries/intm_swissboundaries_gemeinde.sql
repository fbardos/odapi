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
    from {{ ref('stgn_swissboundaries_gemeinde') }}
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

, unique_non_null_bezirk as (
    select distinct
        gemeinde_bfs_id
        , first_value(bezirk_bfs_id) OVER (
            partition by gemeinde_bfs_id
            order by
            case
                when bezirk_bfs_id is null then 2
                else 1
            end,
            snapshot_date desc
        ) as bezirk_bfs_id_replacement
        , first_value(kanton_bfs_id) OVER (
            partition by gemeinde_bfs_id
            order by
            case
                when kanton_bfs_id is null then 2
                else 1
            end,
            snapshot_date desc
        ) as kanton_bfs_id_replacement
    from make_sure_unique
)

, replace_null_with_last_known as(
    select
        src.snapshot_date
        , src.gemeinde_bfs_id
        , src.gemeinde_hist_bfs_id
        , src.gemeinde_name
        , coalesce(src.bezirk_bfs_id, repl.bezirk_bfs_id_replacement) as bezirk_bfs_id
        , coalesce(src.kanton_bfs_id, repl.kanton_bfs_id_replacement) as kanton_bfs_id
        , src.geometry
    from make_sure_unique src
        left join unique_non_null_bezirk repl on
            src.gemeinde_bfs_id = repl.gemeinde_bfs_id

)

select *
from replace_null_with_last_known
