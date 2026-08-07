{% snapshot snap_tax_quartier_winti %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'hh_ek_steuerbar_p10'
            , 'hh_ek_steuerbar_p25'
            , 'hh_ek_steuerbar_p50'
            , 'hh_ek_steuerbar_p75'
            , 'hh_ek_steuerbar_p90'
            , 'hh_verm_steuerbar_p10'
            , 'hh_verm_steuerbar_p25'
            , 'hh_verm_steuerbar_p50'
            , 'hh_verm_steuerbar_p75'
            , 'hh_verm_steuerbar_p90'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'jahr'
            , 'geom_code'
            , 'quartier_bfs_nr'
            , 'stadtkreis_bfs_nr'
            , 'hh_typ_nr'
            , 'einheit_nr'
        ]) }} as sk
    from {{ ref('stgn_tax_quartier_winti') }}
)

select * from src

{% endsnapshot %}
