with src as (
    select
        bfs_indicateur_id::TEXT
        , geo_value::INT
        , geo_name::TEXT
        , indicator_value::NUMERIC
        , group_name::TEXT
        , indicator_name::TEXT
        , indicator_unit::TEXT
        , source::TEXT
        , map_id::TEXT
        , bfs_nivgeo::TEXT
        , TRIM(period_ref)::TEXT as period_ref
        , _source_name::TEXT
        , _source_time::TEXT
    from {{ source('src', 'bfs_statatlas_v2') }}
)

, intm as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key(['map_id', 'group_name', 'geo_value', 'bfs_nivgeo', 'period_ref', 'bfs_indicateur_id']) }} as _surrogate_key
    from src
)

select * from intm
