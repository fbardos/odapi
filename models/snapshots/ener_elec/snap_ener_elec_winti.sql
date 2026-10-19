{% snapshot snap_ener_elec_winti %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeitpunkt'
        ]) }} as sk
    from {{ ref('stgn_ener_elec_winti') }}
)

select * from src

{% endsnapshot %}
