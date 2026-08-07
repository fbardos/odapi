{% snapshot snap_traffic_miv_stgallen %}
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
            , 'andere'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'ort_id'
            , 'datum'
            , 'richtung'
            , 'swiss10group'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_stgallen') }}
)

select * from src

{% endsnapshot %}
