{% snapshot snap_sss_bevoelkerung_wohnbevoelkerung %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'staendig_total_yyyy'
            , 'staendig_total_vor_yyyy_minus_10'
            , 'staendig_veraenderung_10_jahre'
            , 'staendig_bevoelkerungsdichte_yyyy'
            , 'volkszaehlung_2000'
            , 'volkszaehlung_1990'
            , 'volkszaehlung_1980'
            , 'volkszaehlung_1970'
            , 'volkszaehlung_1930'
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_wohnbevoelkerung') }}

{% endsnapshot %}
