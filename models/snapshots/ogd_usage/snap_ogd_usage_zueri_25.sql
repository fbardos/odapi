{% snapshot snap_ogd_usage_zueri_25 %}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'date',
            'dataset_name',
            'resource_id',
            'resource_name',
            'user_agent',
            'url'
        ]) }} as sk
    from {{ ref('stgn_ogd_usage_zueri_25') }}
)

select * from src

{% endsnapshot %}
