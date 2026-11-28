{% snapshot snap_env_tree_zg %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='id',
        check_cols=[
            'gml_id',
            'gemeinde',
            'gemeinde_bfs_id',
            'standort',
            'hausnummer',
            'eigentuemer',
            'bnr',
            'typ',
            'pflanzenname_botanisch',
            'pflanzenname_deutsch',
            'hoehe',
            'kronendurchmesser',
            'stammumfang',
            'pflanzjahr',
            'schutzwuerdigkeit',
            'geometry',
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_env_tree_zg') }}
{% endsnapshot %}
