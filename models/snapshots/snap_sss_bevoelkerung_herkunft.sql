{% snapshot snap_sss_bevoelkerung_herkunft %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'auslaender',
            'auslaender_anteil',
            'eu_efta_total',
            'deutschland',
            'frankreich',
            'italien',
            'oesterreich',
            'spanien',
            'portugal',
            'uebrige_eu_total',
            'serbien',
            'tuerkei',
            'nordmazedonien',
            'russland',
            'asien_total',
            'sri_lanka',
            'indien',
            'china',
            'afrika',
            'nord_sued_amerika',
            'australasien',
            'andere_laender',
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_herkunft') }}

{% endsnapshot %}
