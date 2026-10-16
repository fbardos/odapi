{% snapshot snap_ener_elec_bs %}
with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'zeitpunkt'
        ]) }} as sk
    from {{ ref('stgn_ener_elec_bs') }}
)

select * from src

{% endsnapshot %}
