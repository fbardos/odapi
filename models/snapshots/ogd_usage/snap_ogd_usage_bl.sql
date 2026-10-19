{% snapshot snap_ogd_usage_bl %}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'date',
            'dataset_id',
        ]) }} as sk
    from {{ ref('stgn_ogd_usage_bl') }}
)

select * from src

{% endsnapshot %}
