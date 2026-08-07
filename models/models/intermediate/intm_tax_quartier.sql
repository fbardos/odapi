with src_bs as (
    select
        tax.steuerjahr              ::SMALLINT              as steuerjahr
        , 2701                      ::SMALLINT              as gemeinde_bfs_id
        , 'Basel'                   ::TEXT                  as gemeinde
        , tax.wohnviertel           ::SMALLINT              as quartier_bfs_id
        , tax.wohnviertel_name      ::TEXT                  as quartier
        , tax.anzahl_veranlagungen  ::INT                   as steuerpflichtige
        , tax.steuerbares_einkommen_median::INT
        , tax.steuerbares_vermögen_median::INT              as steuerbares_vermoegen_median
        , tax.dbt_valid_from
        , tax.dbt_valid_to
        , tax.file_source
        , qu.geometry                                       as geom
    from {{ ref('snap_tax_quartier_bs') }} tax
        left join {{ ref('stgn_bfs_quartier') }} qu on
            qu.gemeinde_bfs_id = 2701
            and tax.wohnviertel = qu.quartier_bfs_id
)

, src_winti as (
    select
        tax.jahr                    ::SMALLINT              as steuerjahr
        , 230                       ::SMALLINT              as gemeinde_bfs_id
        , 'Winterthur'              ::TEXT                  as gemeinde
        , tax.quartier_bfs_nr       ::SMALLINT              as quartier_bfs_id
        , tax.quartier              ::TEXT                  as quartier
        , n                         ::INT                   as steuerpflichtige
        , tax.hh_ek_steuerbar_p50   ::INT                   as steuerbares_einkommen_median
        , tax.hh_verm_steuerbar_p50 ::INT                   as steuerbares_vermoegen_median
        , tax.dbt_valid_from
        , tax.dbt_valid_to
        , tax.file_source
        , qu.geometry                                       as geom
    from {{ ref('snap_tax_quartier_winti') }} tax
        left join {{ ref('stgn_bfs_quartier') }} qu on
            qu.gemeinde_bfs_id = 230
            and tax.quartier_bfs_nr = qu.quartier_bfs_id
    where
        geom_code = 'quartier'
        and einheit_nr = 1  -- pro Haushalt
        and hh_typ_nr = 0   -- alle Haushalte
)

, union_data as (
    select * from src_bs
    UNION ALL
    select * from src_winti
)

select
    *
    , ST_Y(ST_Centroid(geom)) as lat
    , ST_X(ST_Centroid(geom)) as lon
from union_data
order by steuerjahr desc, gemeinde_bfs_id, quartier_bfs_id

