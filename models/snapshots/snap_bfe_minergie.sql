{% snapshot snap_bfe_minergie %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='gemeinde_bfs_id',
        check_cols=[
            'anzahl_minergie'
            , 'anzahl_minergie_eco'
            , 'anzahl_minergie_a'
            , 'anzahl_minergie_a_eco'
            , 'anzahl_minergie_p'
            , 'anzahl_minergie_p_eco'
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_bfe_minergie') }}

{% endsnapshot %}
