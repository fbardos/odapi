{% snapshot snap_traffic_miv_bs %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'valuesapproved'
            , 'valuesedited'
            , 'total'
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
            'zst_nr_numerisch_'
            , 'sitecode'
            , 'directionname'
            , 'lanecode'
            , 'date'
            , 'hourfrom'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_bs') }}
)

select * from src

{% endsnapshot %}
