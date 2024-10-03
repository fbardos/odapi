{% snapshot snap_ktzh_gp_auslaenderanteil %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'set_name',
            'thema_name',
            'gebiet_name',
            'subset_name',
            'einheit_kurz',
            'einheit_lang',
            'indikator_name',
            'indikator_value',
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_ktzh_gp_auslaenderanteil') }}
{% endsnapshot %}
