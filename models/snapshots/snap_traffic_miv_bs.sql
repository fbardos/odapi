{% snapshot snap_traffic_miv_bs %}
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
