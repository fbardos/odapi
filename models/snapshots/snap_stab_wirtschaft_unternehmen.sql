{% snapshot snap_stab_wirtschaft_unternehmen %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surr_key',
        check_cols=[
            'indikator',
            'wirtschaftssektor',
        ],
        invalidate_hard_deletes=True,
    )
}}

select *
from {{ ref('stgn_stab_wirtschaft_unternehmen') }}

{% endsnapshot %}
