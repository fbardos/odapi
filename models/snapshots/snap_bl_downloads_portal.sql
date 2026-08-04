{% snapshot snap_bl_downloads_portal %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='datum',
        check_cols=[
            'visitors'
            , 'interactions'
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_bl_downloads_portal') }}

{% endsnapshot %}
