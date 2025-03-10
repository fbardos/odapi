{% snapshot snap_sss_bevoelkerung_geschlecht_zivilstand %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'frauen'
            , 'maenner'
            , 'ledig'
            , 'verheiratet_eingetragen_partnerschaft'
            , 'verwitwet'
            , 'geschieden'
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_sss_bevoelkerung_geschlecht_zivilstand') }}

{% endsnapshot %}
