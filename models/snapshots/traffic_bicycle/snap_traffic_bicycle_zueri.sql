{% snapshot snap_traffic_bicycle_zueri %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'standort_id'
            , 'datum'
        ]) }} as sk
    from {{ ref('stgn_traffic_bicycle_zueri') }}
)

select * from src

{% endsnapshot %}
