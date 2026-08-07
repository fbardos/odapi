-- TODO: Add information about source
-- TODO: Add information about license
-- TODO: Add information about gemeinde/kanton_bfs_id, ...
with src_tg as (
    select
        code                    ::TEXT          as anlage_id
        , richtung              ::TEXT          as richtung
        , spur_code             ::TEXT          as spur
        , (datum::DATE + zeit_von::TIME) ::TIMESTAMP     as zeit_von
        , (datum::DATE + zeit_bis::TIME) ::TIMESTAMP     as zeit_bis
        , mr                    ::INT           as mr
        , pw                    ::INT           as pw
        , pw_                   ::INT           as pw_p
        , lief                  ::INT           as lief
        , lief_                 ::INT           as lief_p
        , lief_aufl_            ::INT           as lief_a
        , lw                    ::INT           as lw
        , lw_                   ::INT           as lw_p
        , sattelzug             ::INT           as satt
        , bus                   ::INT           as bus
        , NULL                  ::INT           as andere
        , ST_SetCRS(
            ST_Point(
                trim(split_part(koordinaten, ',', 2))::double precision,
                split_part(koordinaten, ',', 1)::double precision
            )
            , 'EPSG:4326'
        ) as geom
        , dbt_valid_from        ::TIMESTAMP
        , dbt_valid_to          ::TIMESTAMP
        , file_source           ::TEXT
    from {{ ref('snap_traffic_miv_tg') }}
)

, src_bs as (
    select
        zst_nr_text_            ::TEXT          as anlage_id
        , directionname         ::TEXT          as richtung
        , lanename              ::TEXT          as spur
        , datetimefrom          ::TIMESTAMP     as zeit_von
        , datetimeto            ::TIMESTAMP     as zeit_bis
        , mr                    ::INT           as mr
        , pw                    ::INT           as pw
        , pw_                   ::INT           as pw_p
        , lief                  ::INT           as lief
        , lief_                 ::INT           as lief_p
        , lief_aufl_            ::INT           as lief_a
        , lw                    ::INT           as lw
        , lw_                   ::INT           as lw_p
        , sattelzug             ::INT           as satt
        , bus                   ::INT           as bus
        , andere                ::INT           as andere
        , ST_SetCRS(
            ST_Point(
                trim(split_part(geo_point, ',', 2))::double precision,
                split_part(geo_point, ',', 1)::double precision
            )
            , 'EPSG:4326'
        ) as geom
        , dbt_valid_from        ::TIMESTAMP
        , dbt_valid_to          ::TIMESTAMP
        , file_source           ::TEXT
    from {{ ref('snap_traffic_miv_bs') }}
)

, src_winti as (
    select
        anlage_nr               ::TEXT          as anlage_id
        , richtung_name         ::TEXT          as richtung
        , spur_nr               ::TEXT          as spur
        , zeit_von              ::TIMESTAMP     as zeit_von
        , zeit_bis              ::TIMESTAMP     as zeit_bis
        , mr                    ::INT           as mr
        , pw                    ::INT           as pw
        , pw_plus               ::INT           as pw_p
        , lief                  ::INT           as lief
        , lief_plus             ::INT           as lief_p
        , lief_aufl             ::INT           as lief_a
        , lw                    ::INT           as lw
        , lw_plus               ::INT           as lw_p
        , sattelzug             ::INT           as satt
        , bus                   ::INT           as bus
        , NULL                  ::INT           as andere
        , ST_SetCRS(
            ST_Point(
                lat
                , lon
            )
            , 'EPSG:4326'
        ) as geom
        , dbt_valid_from        ::TIMESTAMP
        , dbt_valid_to          ::TIMESTAMP
        , file_source           ::TEXT
    from {{ ref('snap_traffic_miv_winti') }}
)

, src_stgallen as (
    select 
        ort_id
        , bezeichnung
        , richtung
        , standort
        , datum
        , stunde
        , dbt_valid_from
        , dbt_valid_to
        , file_source
        , {{ dbt_utils.pivot(
              'swiss10group',
              dbt_utils.get_column_values(ref('intm_traffic_miv_stgallen_unpivot'), 'swiss10group')
        ) }}
    from {{ ref('intm_traffic_miv_stgallen_unpivot') }}
    group by
        ort_id
        , bezeichnung
        , richtung
        , standort
        , datum
        , stunde
        , dbt_valid_from
        , dbt_valid_to
        , file_source
)

, intm_stgallen as (
    select
        ort_id                  ::TEXT          as anlage_id
        , richtung              ::TEXT          as richtung
        , 0                     ::TEXT          as spur
        , datum::DATE + interval '1 hour' * (stunde::INT - 1) as zeit_von
        , datum::DATE + interval '1 hour' * (stunde::INT) - interval '1 second' as zeit_bis
        , "MR"                  ::INT           as mr
        , "PW"                  ::INT           as pw
        , "PWAN"                ::INT           as pw_p
        , "LI"                  ::INT           as lief
        , "LIAN"                ::INT           as lief_p
        , "LIAU"                ::INT           as lief_a
        , "LW"                  ::INT           as lw
        , "LZ"                  ::INT           as lw_p
        , "SZ"                  ::INT           as satt
        , "CA"                  ::INT           as bus
        , NULL                  ::INT           as andere
        , ST_SetCRS(
            ST_Point(
               split_part(standort, ',', 1)::double precision   -- lat / y
               , split_part(standort, ',', 2)::double precision -- lon / x
            )
            , 'EPSG:4326'
        ) as geom
        , dbt_valid_from        ::TIMESTAMP
        , dbt_valid_to          ::TIMESTAMP
        , file_source           ::TEXT
    from src_stgallen
)

, union_data as (
    select * from src_tg
    UNION ALL
    select * from src_bs
    UNION ALL
    select * from src_winti
    UNION ALL
    select * from intm_stgallen
)

select
    *
    , ST_Y(geom) as lat
    , ST_X(geom) as lon
from union_data
