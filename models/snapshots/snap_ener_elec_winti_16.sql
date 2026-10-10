{% snapshot snap_ener_elec_winti_16 %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeitpunkt'
        ]) }} as sk
    from {{ ref('stgn_ener_elec_winti_16') }}
)

select * from src

{% endsnapshot %}
