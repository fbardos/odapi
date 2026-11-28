{% snapshot snap_env_tree_zueri %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='baumnummer',
        check_cols=[
            'gml_id',
            'poi_id',
            'kategorie',
            'quartier',
            'strasse',
            'baumgattunglat',
            'baumartlat',
            'baumnamelat',
            'baumnamedeu',
            'status',
            'baumtyp',
            'baumtyptext',
            'kronendurchmesser',
            'pflanzjahr',
            'genauigkeit',
            'geometry',
        ],
        invalidate_hard_deletes=True,
    )
}}
select *
from {{ ref('stgn_env_tree_zueri') }}
{% endsnapshot %}
