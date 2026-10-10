with src_bs as (
    select
        zeitpunkt_utc as zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_bs') }}
)

, src_zueri as (
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_19') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_20') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_21') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_22') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_23') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_24') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_25') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_zueri_26') }}
)

, src_winti as (
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_winti') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_winti_19') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_winti_16') }}
    UNION ALL
    select
        zeitpunkt
        , bruttolastgang_kwh
        -- TODO: Add geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
        , geom_level
        , geom_id
    from {{ ref('snap_ener_elec_winti_13') }}
)

, union_publisher as (
    select * from src_bs
    UNION ALL
    select * from src_zueri
    UNION ALL
    select * from src_winti
)

select *
from union_publisher
