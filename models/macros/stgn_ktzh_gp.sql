{% macro stgn_ktzh_gp(source_table_name) %}
with src as (
    select
        "BFS_NR"::INTEGER as bfs_nr
        , "SET_NAME" as set_name
        , "THEMA_NAME" as thema_name
        , "GEBIET_NAME" as gebiet_name
        , "SUBSET_NAME" as subset_name
        , "EINHEIT_KURZ" as einheit_kurz
        , "EINHEIT_LANG" as einheit_lang
        , "INDIKATOR_ID"::INTEGER as indikator_id
        , "INDIKATOR_JAHR"::INTEGER as indikator_jahr
        , "INDIKATOR_NAME" as indikator_name
        , "INDIKATOR_VALUE"::INTEGER as indikator_value
        -- TODO: Add source_org and source_url
        -- By gaining data from the source, work with opendata.swiss metadata to gain this information.
        -- And insert it directly into the source table.
    from {{ source('src', source_table_name) }}
)
, intm as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key(['bfs_nr', 'indikator_jahr', 'indikator_id']) }} as _surrogate_key
    from src
    where
        bfs_nr != 0  -- temporary exclude Bezirk, Region, ...
)
select *
from intm
{% endmacro %}
