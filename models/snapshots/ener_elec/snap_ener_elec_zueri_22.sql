{% snapshot snap_ener_elec_zueri_22 %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeitpunkt'
        ]) }} as sk
    from {{ ref('stgn_ener_elec_zueri_22') }}
)

select * from src

{% endsnapshot %}
