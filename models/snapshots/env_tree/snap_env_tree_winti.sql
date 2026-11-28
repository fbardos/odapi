{% snapshot snap_env_tree_winti %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='baumnummer',
        check_cols=[
            'baumtyp',
            'baumart_d',
            'baumart_l',
            'baumhoehe',
            'faellung',
            'fid',
            'groesse_darstellung',
            'kronendurchmesser',
            'pflanzjahr',
            'z',
            'geometry',
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_env_tree_winti') }}
{% endsnapshot %}
