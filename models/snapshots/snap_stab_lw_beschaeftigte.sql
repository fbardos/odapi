{% snapshot snap_stab_lw_beschaeftigte %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surr_key',
        check_cols=[
            'beobachtungseinheit',
            'betriebssystem'
        ],
        invalidate_hard_deletes=True,
    )
}}

select *
from {{ ref('stgn_stab_lw_beschaeftigte') }}

{% endsnapshot %}
