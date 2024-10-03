{% macro intm_ktzh_gp(model) %}
    /*

    Generates INTM table for Gemeindeportrait.

    Snapshot table is automatically determined with the followig logic:
        - name of intm model: intm_ktzh_gp_bevoelkerung
        - name of snap model: snap_ktzh_gp_bevoelkerung

    Dependencies from other models needs to manually set here.
    Cannot be read from config yaml file, because during parsing,
    does not read from there.
    Other options like the indicator can be read from config yaml file.

    Example config inside model config yaml:

        > config:
        >   group: *default-group
        >   odapi:
        >     indicator: 1
        >     period_type: duedate
        >     period_code: year

    */

    {% set snap_name = model.get("name").replace('intm', 'snap', 1) %}

    {% if execute %}
        {% set indicator_id = config.require('odapi').get('indicator', -1) %}
        {% set period_type = config.require('odapi').get('period_type', none) %}
        {% set period_code = config.require('odapi').get('period_code', none) %}
    {% endif %}

    with src as (
        select *
        from {{ ref(snap_name) }}
        -- temporary: only current wissensstand
        where
            dbt_valid_to is NULL
    )
    , mapping as (
        select
            {{ indicator_id }} as indicator_id
            , 'polg' as geo_code  -- politische gemeinde
            , src.bfs_nr as geo_value
            , 'latest' as knowledge_code -- latest
            , src.dbt_valid_from as knowledge_date
            , '{{ period_type }}' as period_type
            , '{{ period_code }}' as period_code
            , NULL::DATE as period_ref_from
            , to_date(concat(src.indikator_jahr, '-12-31'), 'yyyy-mm-dd') as period_ref
            , src.indikator_value as indicator_value
            , 'Gemeindeportrait des Kantons Zürich' as source
        from src
    )
    select * from mapping
{% endmacro %}
