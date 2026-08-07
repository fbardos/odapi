{% snapshot snap_tax_quartier_zueri %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'steuereinkommen_p50'
            , 'steuereinkommen_p25'
            , 'steuereinkommen_p75'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'stichtagdatjahr'
            , 'quarsort'
            , 'steuertarifcd'
        ]) }} as sk
    from {{ ref('stgn_tax_quartier_zueri') }}
)

select * from src

{% endsnapshot %}
