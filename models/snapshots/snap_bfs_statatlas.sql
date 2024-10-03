{% snapshot snap_bfs_statatlas %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'unit',
            'value',
            'status',
            'status_desc',
            'desc_val',
            'source',
            'last_update',
            'geom_period',
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_bfs_statatlas') }}

{% endsnapshot %}
