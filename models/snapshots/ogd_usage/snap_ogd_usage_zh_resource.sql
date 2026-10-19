{% snapshot snap_ogd_usage_zh_resource %}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'datum',
            'datensatz_id',
            'ressourcen_id',
            'download_url',
        ]) }} as sk
    from {{ ref('stgn_ogd_usage_zh_resource') }}
)

select * from src

{% endsnapshot %}
