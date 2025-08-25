{% snapshot snap_bfs_statatlas_v2 %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'geo_name',
            'indicator_value',
            'indicator_unit',
            'source',
        ],
        invalidate_hard_deletes=True,
    )
}}

select *
from {{ ref('stgn_bfs_statatlas_v2') }}

{% endsnapshot %}
