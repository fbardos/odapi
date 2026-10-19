{% snapshot snap_traffic_bicycle_winti %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'anlage_nr'
            , 'zeit_von'
            , 'richtung'
            , 'spur_nr'
        ]) }} as sk
    from {{ ref('stgn_traffic_bicycle_winti') }}
)

select * from src

{% endsnapshot %}
