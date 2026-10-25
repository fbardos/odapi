{% snapshot snap_traffic_bicycle_stgallen %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeit_von'
            , 'anlage_name'
        ]) }} as sk
    from {{ ref('stgn_traffic_bicycle_stgallen') }}
)

select * from src

{% endsnapshot %}
