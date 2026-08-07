{% snapshot snap_traffic_miv_tg %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'reg_bus'
            , 'bus'
            , 'mr'
            , 'pw'
            , 'pw_'
            , 'lief'
            , 'lief_'
            , 'lief_aufl_'
            , 'lw'
            , 'lw_'
            , 'sattelzug'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'code'
            , 'strasse'
            , 'richtung'
            , 'spur_code'
            , 'datum'
            , 'zeit_von'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_tg') }}
)

select * from src

{% endsnapshot %}
