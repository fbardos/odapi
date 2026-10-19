-- XXX: Multiple runs with the same source data will lead to new dbt_valid_from rows why?
{% snapshot snap_traffic_bicycle_bs %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zst_id'
            , 'datetimefrom_utc'
            , 'directionname'
            , 'lanename'
            , 'traffictype'
        ]) }} as sk
    from {{ ref('stgn_traffic_bicycle_bs') }}
)

select * from src

{% endsnapshot %}
