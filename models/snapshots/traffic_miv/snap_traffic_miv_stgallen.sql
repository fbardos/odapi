{% snapshot snap_traffic_miv_stgallen %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'ort_id'
            , 'datum'
            , 'richtung'
            , 'swiss10_group'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_stgallen') }}
)

select * from src

{% endsnapshot %}
