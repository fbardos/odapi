{% snapshot snap_ogd_usage_zh_dataset %}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'datum',
            'datensatz_id',
        ]) }} as sk
    from {{ ref('stgn_ogd_usage_zh_dataset') }}
)

select * from src

{% endsnapshot %}
