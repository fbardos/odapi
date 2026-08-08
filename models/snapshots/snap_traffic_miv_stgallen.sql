{% snapshot snap_traffic_miv_stgallen %}
{{
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='sk',
        check_cols=[
            'bezeichnung'
            , 'standort'
            , 'wochentag'
            , 'arbeitstag'
            , 'kennzahl'
            , 'ort_richtung_id'
            , 'klasse'
            , 'name_d'
            , 'swiss7group'
            , 'swiss6group'
            , '1'
            , '2'
            , '3'
            , '4'
            , '5'
            , '6'
            , '7'
            , '8'
            , '9'
            , '10'
            , '11'
            , '12'
            , '13'
            , '14'
            , '15'
            , '16'
            , '17'
            , '18'
            , '19'
            , '20'
            , '21'
            , '22'
            , '23'
            , '24'
            , 'tagestotal'
        ],
        invalidate_hard_deletes=True,
    )
}}

with src as (
    select
        *
        , {{ dbt_utils.generate_surrogate_key([
            'ort_id'
            , 'datum'
            , 'richtung'
            , 'swiss10group'
        ]) }} as sk
    from {{ ref('stgn_traffic_miv_stgallen') }}
)

select * from src

{% endsnapshot %}
