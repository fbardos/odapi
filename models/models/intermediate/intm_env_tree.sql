with src_zg as (
    select
        id::String                      as id
        , case
            when typ = 'unbekannt' then NUll::Nullable(String)
            else typ::Nullable(String)
        end as typ
        , pflanzenname_deutsch          as art_deu
        , pflanzenname_botanisch        as art_lat
        , hoehe
        , kronendurchmesser             as krone_durchmesser
        , stammumfang                   as stamm_umfang
        , pflanzjahr                    as jahr_pflanzung

        , gemeinde_bfs_id
        , gemeinde

        , geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_env_tree_zg') }}

)

, src_winti as (
    select
        baumnummer::String          as id
        , baumtyp::Nullable(String) as typ
        , baumart_d                 as art_deu
        , baumart_l                 as art_lat
        , baumhoehe                 as hoehe
        , kronendurchmesser         as krone_durchmesser
        , NULL::Nullable(UInt16)    as stamm_umfang
        , NULL::Nullable(UInt16)    as jahr_pflanzung

        , 230::UInt16               as gemeinde_bfs_id
        , 'Winterthur'::String      as gemeinde

        , geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_env_tree_winti') }}
)


, src_zueri as (
    select
        baumnummer::String          as id
        , case
            when baumtyptext = 'nicht zugeordnet' then NULL::Nullable(String)
            else baumtyptext::Nullable(String)
        end as typ
        , baumnamedeu               as art_deu
        , baumnamelat               as art_lat
        , NULL::Nullable(UInt16)    as hoehe
        , kronendurchmesser         as krone_durchmesser
        , NULL::Nullable(UInt16)    as stamm_umfang
        , pflanzjahr                as jahr_pflanzung

        , 261::UInt16               as gemeinde_bfs_id
        , 'Zürich'::String          as gemeinde

        , geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_env_tree_zueri') }}
)

, union_tables as (
    select * from src_zg
    UNION ALL
    select * from src_zueri
    UNION ALL
    select * from src_winti
)

select * from union_tables
