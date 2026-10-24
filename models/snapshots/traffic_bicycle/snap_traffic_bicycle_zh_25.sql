{% snapshot snap_traffic_bicycle_zh_25 %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'anlage_nr'
            , 'zeit_von'
            , 'spur_richtung'
        ]) }} as sk
    from {{ ref('stgn_traffic_bicycle_zh_25') }}
)

select * from src

{% endsnapshot %}
