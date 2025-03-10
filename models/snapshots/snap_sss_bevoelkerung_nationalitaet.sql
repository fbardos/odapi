{% snapshot snap_sss_bevoelkerung_nationalitaet %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'schweizer',
            'auslaender',
            'schweizer_geboren_schweiz',
            'schweizer_geboren_ausland',
            'auslaender_geboren_schweiz',
            'auslaender_geboren_ausland',
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_nationalitaet') }}

{% endsnapshot %}
