{% snapshot snap_tax_quartier_bs %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'anzahl_veranlagungen'
            , 'reineinkommen_mittelwert'
            , 'reineinkommen_median'
            , 'reineinkommen_gini_koeffizient'
            , 'steuerbares_einkommen_mittelwert'
            , 'steuerbares_einkommen_median'
            , 'ertrag_einkommenssteuer_mittelwert'
            , 'ertrag_einkommenssteuer_median'
            , 'reinvermögen_mittelwert'
            , 'reinvermögen_median'
            , 'reinvermögen_gini_koeffizient'
            , 'steuerbares_vermögen_mittelwert'
            , 'steuerbares_vermögen_median'
            , 'ertrag_vermögenssteuer_mittelwert'
            , 'ertrag_vermögenssteuer_median'
            , 'steuerjahr_zahl'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'steuerjahr'
            , 'wohnviertel'
        ]) }} as sk
    from {{ ref('stgn_tax_quartier_bs') }}
)

select * from src

{% endsnapshot %}
