-- Convention: one mart per source?
{% macro intm_bfs_statatlas() %}
    /*

    Generates INTM table for Gemeindeportrait.


    Dependencies from other models needs to manually set here.
    Cannot be read from config yaml file, because during parsing,
    does not read from there.
    Other options like the indicator can be read from config yaml file.

    Example config inside model config yaml:

        > config:
        >   group: *default-group
        >   odapi:
        >     indicator: 3
        >     period_type: duedate
        >     period_code: year
        >   intm_statatlas:
        >     indicator_regex: 'Anzahl Einwohner/innen'
        >     # no ref tranformation needed
        >     # period_ref_from_transformation:
        >     # period_ref_transformation: tests:

    */

    {% if execute %}
        {% set indicator_id = config.require('odapi').get('indicator', -1) %}
        {% set period_type = config.require('odapi').get('period_type', none) %}
        {% set period_code = config.require('odapi').get('period_code', none) %}
        {% set indicator_regex = config.require('intm_statatlas').get('indicator_regex', -1) %}
        {% set path_regex = config.require('intm_statatlas').get('path_regex', []) %}
        {% set period_ref_from_transformation = config.require('intm_statatlas').get('period_ref_from_transformation', none) %}
        {% set period_ref_transformation = config.require('intm_statatlas').get('period_ref_transformation', none) %}
        {% set distinct_rows = config.require('intm_statatlas').get('distinct_rows', none) %}
        {% set value_col = config.require('intm_statatlas').get('value_col', 'value') %}
        {% set value_col_is_text = config.require('intm_statatlas').get('value_col_is_text', false) %}
    {% endif %}

with src as (
    select *
    from {{ ref('snap_bfs_statatlas') }}
    where
        geom_code in (
            'polg'     -- politische gemeinde
            , 'bezk'   -- bezirk
            , 'kant'   -- kanton
        )
        -- and dbt_valid_to is NULL  -- temporary: only current wissensstand
)
, filter as (
    select *
    from src
    where
        variable ~ '{{ indicator_regex }}'
        -- Multiple regex rules allowed (logical OR)
        {% if path_regex|length > 0 %}
            and (
                0=1
                {% for rule in path_regex %}
                    OR mother_path ~ '{{ rule }}'
                {% endfor %}
            )
        {% endif %}
)
, mapping as (
    select
    {% if distinct_rows %}
        distinct
    {% endif %}
        {{ indicator_id }} as indicator_id
        , geom_code as geo_code  -- politische gemeinde
        , geo_id::INTEGER as geo_value
        , dbt_valid_from as knowledge_date_from
        , dbt_valid_to as knowledge_date_to
        , '{{ period_type }}' as period_type
        , '{{ period_code }}' as period_code
        {% if period_ref_from_transformation %}
            , {{ period_ref_from_transformation }} as period_ref_from
        {% else %}
            , NULL::DATE as period_ref_from
        {% endif %}
        {% if period_ref_transformation %}
            , {{ period_ref_transformation }} as period_ref
        {% else %}
            , period_ref::DATE as period_ref
        {% endif %}
        {% if value_col_is_text %}
            , NULL::NUMERIC as indicator_value_numeric
            , {{ value_col }}::TEXT as indicator_value_text
        {% else %}
            , {{ value_col }}::NUMERIC as indicator_value_numeric
            , NULL::TEXT as indicator_value_text
        {% endif %}
        , source
    from filter
)
select * from mapping
{% endmacro %}
