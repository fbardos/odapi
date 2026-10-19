{% snapshot snap_traffic_miv_tg %}
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
