{% snapshot snap_sss_bevoelkerung_bilanz %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'wohnbevoelkerung_jahresanfang',
            'lebendgeburten',
            'todesfaelle',
            'geburtenueberschuss',
            'zuzug',
            'wegzug',
            'wanderungssaldo',
            'bev_gesamtbilanz',
            'bev_gesamtbilanz_prozent',
            'wohnbevoelkerung_jahresende',
            'lebendgeburten_pro_1000',
            'todesfaelle_pro_1000',
            'geburtenueberschuss_pro_1000',
            'zuzug_pro_1000',
            'wegzug_pro_1000',
            'wanderungssaldo_pro_1000',
            'bev_gesamtbilanz_pro_1000',
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_bilanz') }}

{% endsnapshot %}
