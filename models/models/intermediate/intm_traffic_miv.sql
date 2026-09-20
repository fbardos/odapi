-- TODO: Add information about source
-- TODO: Add information about license
-- TODO: Add information about gemeinde/kanton_bfs_id, ...
with src_tg as (
    select
        toString(code) as anlage_id
        , toString(richtung) as richtung
        , toString(spur_code) as spur
        , parseDateTimeOrNull(
            concat(toString(toDate(datum)), ' ', nullIf(trim(zeit_von), '')),
            '%Y-%m-%d %H:%i:%s'
        ) as zeit_von

        , parseDateTimeOrNull(
            concat(toString(toDate(datum)), ' ', nullIf(trim(zeit_bis), '')),
            '%Y-%m-%d %H:%i:%s'
        ) as zeit_bis
        , toInt32(mr) as mr
        , toInt32(pw) as pw
        , toInt32(pwx) as pw_p
        , toInt32(lief) as lief
        , toInt32(liefx) as lief_p
        , toInt32(liefxauflx) as lief_a
        , toInt32(lw) as lw
        , toInt32(lwx) as lw_p
        , toInt32(sattelzug) as satt
        , toInt32(bus) as bus
        , cast(NULL as Nullable(Int32)) as andere
        , (
            toFloat64(trim(splitByChar(',', koordinaten)[2]))
            , toFloat64(trim(splitByChar(',', koordinaten)[1]))
        )::Point as geom
        , toDateTime(dbt_valid_from) as dbt_valid_from
        , toDateTime(dbt_valid_to) as dbt_valid_to
        -- , toString(file_source) as file_source
    from {{ ref('snap_traffic_miv_tg') }}
)

, src_bs as (
    select
        toString(zst_nr_textx) as anlage_id
        , toString(direction_name) as richtung
        , toString(lane_name) as spur
        , toDateTime(date_time_from) as zeit_von
        , toDateTime(date_time_to) as zeit_bis
        , toInt32(mr) as mr
        , toInt32(pw) as pw
        , toInt32(p_wx) as pw_p
        , toInt32(lief) as lief
        , toInt32(liefx) as lief_p
        , toInt32(liefx_auflx) as lief_a
        , toInt32(lw) as lw
        , toInt32(l_wx) as lw_p
        , toInt32(sattelzug) as satt
        , toInt32(bus) as bus
        , toInt32(andere) as andere
        , (
            coalesce(
                toFloat64OrNull(trim(splitByChar(',', geo_point)[2])),
                0.0
            ),
            coalesce(
                toFloat64OrNull(trim(splitByChar(',', geo_point)[1])),
                0.0
            )
        )::Point as geom
        , toDateTime(dbt_valid_from) as dbt_valid_from
        , toDateTime(dbt_valid_to) as dbt_valid_to
        -- , toString(file_source) as file_source
    from {{ ref('snap_traffic_miv_bs') }}
)

, src_winti as (
    select
        toString(anlage_nr) as anlage_id
        , toString(richtung_name) as richtung
        , toString(spur_nr) as spur
        , toDateTime(zeit_von) as zeit_von
        , toDateTime(zeit_bis) as zeit_bis
        , toInt32(mr) as mr
        , toInt32(pw) as pw
        , toInt32(pw_plus) as pw_p
        , toInt32(lief) as lief
        , toInt32(lief_plus) as lief_p
        , toInt32(lief_aufl) as lief_a
        , toInt32(lw) as lw
        , toInt32(lw_plus) as lw_p
        , toInt32(sattelzug) as satt
        , toInt32(bus) as bus
        , cast(NULL as Nullable(Int32)) as andere
        , (
            toFloat64(lon)
            , toFloat64(lat)
        )::Point as geom
        , toDateTime(dbt_valid_from) as dbt_valid_from
        , toDateTime(dbt_valid_to) as dbt_valid_to
        -- , toString(file_source) as file_source
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
        -- , file_source
        , {{ dbt_utils.pivot(
              'swiss10_group',
              dbt_utils.get_column_values(ref('intm_traffic_miv_stgallen_unpivot'), 'swiss10_group')
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
        -- , file_source
)

, src_stgallen_clean as (
    select
        *
        , toFloat64OrNull(trim(splitByChar(',', standort)[1])) as lat
        , toFloat64OrNull(trim(splitByChar(',', standort)[2])) as lon
    from src_stgallen
)

, intm_stgallen as (
    select
        toString(ort_id) as anlage_id
        , toString(richtung) as richtung
        , toString(0) as spur
        , toDateTime(datum)
            + toIntervalHour(
                toInt32OrNull(replaceRegexpAll(stunde, '[^0-9]', '')) - 1
            ) as zeit_von
        , toDateTime(datum)
            + toIntervalHour(
                toInt32OrNull(replaceRegexpAll(stunde, '[^0-9]', ''))
            )
            - toIntervalSecond(1) as zeit_bis
        , toInt32(`MR`) as mr
        , toInt32(`PW`) as pw
        , toInt32(`PWAN`) as pw_p
        , toInt32(`LI`) as lief
        , toInt32(`LIAN`) as lief_p
        , toInt32(`LIAU`) as lief_a
        , toInt32(`LW`) as lw
        , toInt32(`LZ`) as lw_p
        , toInt32(`SZ`) as satt
        , toInt32(`CA`) as bus
        , cast(NULL as Nullable(Int32)) as andere
        , (assumeNotNull(lon), assumeNotNull(lat))::Point as geom
        , parseDateTimeBestEffortOrNull(toString(dbt_valid_from)) as dbt_valid_from
        , parseDateTimeBestEffortOrNull(toString(dbt_valid_to)) as dbt_valid_to
        -- , toString(file_source) as file_source
    from src_stgallen_clean
    where lat is not null
      and lon is not null
      and toInt32OrNull(replaceRegexpAll(stunde, '[^0-9]', '')) is not null
)

, union_data as (
    select * from src_tg
    union all
    select * from src_bs
    union all
    select * from src_winti
    union all
    select * from intm_stgallen
)

select
    *
    , geom.2 as lat
    , geom.1 as lon
from union_data
