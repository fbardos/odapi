-- TODO: Some sources also contain pedestrians,
-- add as another mart
with src_zueri as (
    -- TODO: Should load https://opendata.swiss/de/dataset/standorte-der-automatischen-fuss-und-velozahlungen
    -- for additional information about richtung in/out, spur, ...
    select
        standort_id::String as standort_id
        -- TODO: Make sure UTC
        , datum as zeit_von
        , velo_in
        , velo_out
        -- , fuss_in
        -- , fuss_out
        , accurateCastOrNull(tuple(ost, nord), 'Point') as geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_traffic_bicycle_zueri') }}
)

, src_winti as (
    select
        anlage_nr::String as standort_id
        -- XXX: Make sure UTC
        , zeit_von
        , richtung_name as richtung
        , spur_nr as spur_id
        , anzahl as velos
        , accurateCastOrNull(tuple(lon, lat), 'Point') as geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_traffic_bicycle_winti') }}
)

, src_bs as (
    select
        zst_id::String as standort_id
        , datetimefrom_utc as zeit_von
        , directionname as richtung
        , lanecode as spur_id
        , total as velos
        -- , geo_point_2d as geometry
        -- hacky
        , accurateCastOrNull(tuple(geo_point_2d.1, geo_point_2d.2), 'Point') as geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from {{ ref('snap_traffic_bicycle_bs') }}
    where
        traffictype = 'Velo'
)


, intm_zueri as (
    select
        standort_id
        , zeit_von
        , 'in'::String as richtung
        , 1 as spur_id
        , velo_in as velos
        , geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from src_zueri
    UNION ALL
    select
        standort_id
        , zeit_von
        , 'out' as richtung
        , 2 as spur_id
        , velo_out as velos
        , geometry
        , file_source
        , publisher
        , dbt_valid_from
        , dbt_valid_to
    from src_zueri
)

, union_publisher as (
    select * from intm_zueri
    UNION ALL
    select * from src_winti
    UNION ALL
    select * from src_bs
)

select *
from union_publisher
