{% snapshot snap_traffic_miv_bs %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'values_approved'
            , 'values_edited'
            , 'total'
            , 'bus'
            , 'mr'
            , 'pw'
            , 'p_wx'
            , 'lief'
            , 'liefx'
            , 'liefx_auflx'
            , 'lw'
            , 'l_wx'
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
            'zst_nr_numerischx'
            , 'site_code'
            , 'direction_name'
            , 'lane_code'
            , 'date'
            , 'hour_from'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_bs') }}
)

select * from src

{% endsnapshot %}
