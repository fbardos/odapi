{% snapshot snap_ener_elec_zueri_25 %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeitpunkt'
        ]) }} as sk
    from {{ ref('stgn_ener_elec_zueri_25') }}
)

select * from src

{% endsnapshot %}
