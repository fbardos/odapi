{% snapshot snap_sss_bevoelkerung_altersklassen %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'total',
            'age0_4',
            'age5_9',
            'age10_14',
            'age15_19',
            'age20_24',
            'age25_29',
            'age30_34',
            'age35_39',
            'age40_44',
            'age45_49',
            'age50_54',
            'age55_59',
            'age60_64',
            'age65_69',
            'age70_74',
            'age75_79',
            'age80_84',
            'age85_89',
            'age90_94',
            'age95_99',
            '100_plus',
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_altersklassen') }}

{% endsnapshot %}
