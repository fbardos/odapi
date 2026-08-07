{% snapshot snap_traffic_miv_winti %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'bus'
            , 'mr'
            , 'pw'
            , 'pw_plus'
            , 'lief'
            , 'lief_plus'
            , 'lief_aufl'
            , 'lw'
            , 'lw_plus'
            , 'sattelzug'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'anlage_nr'
            , 'zeit_von'
            , 'richtung'
            , 'spur_nr'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_winti') }}
)

select * from src

{% endsnapshot %}
