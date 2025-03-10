{% snapshot snap_sss_bevoelkerung_aufenthalt %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'auslaender',
            'aufenthalt_b',
            'niedergelassen_c',
            'vorl_aufgenommen_f',
            'kurzaufenthalter_l',
            'asylsuchend_n',
            'schutzstatus_s',
            'diplomat',
            'andere',
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_aufenthalt') }}

{% endsnapshot %}
