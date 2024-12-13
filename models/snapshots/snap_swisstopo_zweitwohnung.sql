{% snapshot snap_swisstopo_zweitwohnung %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='_surrogate_key',
        check_cols=[
            'wohnungen'
            , 'erstwohnungen'
            , 'erstwohnung_gleichgestellt'
            , 'anteil_erstwohnung'
            , 'anteil_zweitwohnung'
            , 'verfahren_old'
            , 'verfahren_new'
            , 'status'
        ],
        invalidate_hard_deletes=False,
    )
}}
select *
from {{ ref('stgn_swisstopo_zweitwohnung') }}

{% endsnapshot %}
