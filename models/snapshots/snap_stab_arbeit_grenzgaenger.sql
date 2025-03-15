{% snapshot snap_stab_arbeit_grenzgaenger %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surr_key',
        check_cols=[
            'indicator_value',
            'source',
        ],
        invalidate_hard_deletes=True,
    )
}}

select *
from {{ ref('stgn_stab_arbeit_grenzgaenger') }}

{% endsnapshot %}
