{% snapshot snap_stats_whg_gebkat %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'action',
            'freq',
            'obs_value',
            'diff_region_state',
            'diff_embargo_date',
            'diff_db_state',
            'obs_status',
            'diff_region_ref',
            'source',
        ],
        updated_at='diff_last_update',
        invalidate_hard_deletes=True,
    )
}}

select *
from {{ ref('stgn_stats_whg_gebkat') }}

{% endsnapshot %}
