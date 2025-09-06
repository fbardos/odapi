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
        {% set indicator_regex = config.get('intm_statatlas', {}).get('indicator_regex', -1) %}
        {% set path_regex = config.get('intm_statatlas', {}).get('path_regex', []) %}
        {% set period_ref_from_transformation = config.get('intm_statatlas', {}).get('period_ref_from_transformation', none) %}
        {% set period_ref_transformation = config.get('intm_statatlas', {}).get('period_ref_transformation', none) %}
        {% set distinct_rows = config.get('intm_statatlas', {}).get('distinct_rows', none) %}
        {% set value_col = config.get('intm_statatlas', {}).get('value_col', 'value') %}
        {% set value_col_is_text = config.get('intm_statatlas', {}).get('value_col_is_text', false) %}
        {% set v1_config = config.get('intm_statatlas', none) %}
        {% set v2_config = config.get('intm_statatlas_v2', none) %}
        {% set v2_indicateur_id = config.get('intm_statatlas_v2', {}).get('indicateur_id', none) %}
        {% set v2_map_id = config.get('intm_statatlas_v2', {}).get('map_id', none) %}
        {% set v2_group_regex = config.get('intm_statatlas_v2', {}).get('group_regex', -1) %}
        {% set v2_period_ref_from_transformation = config.get('intm_statatlas_v2', {}).get('period_ref_from_transformation', none) %}
        {% set v2_period_ref_transformation = config.get('intm_statatlas_v2', {}).get('period_ref_transformation', none) %}
		{% set v2_grouping = config.get('intm_statatlas_v2', {}).get('grouping', []) %}
    {% endif %}

with _dummy as (
    select 1 as dummy
)

{% if v2_config is not none %}
    -- this CTE is used to combine the two
    -- snapshot tables together.
    , min_dbt_validity_v2 as (
        select min(dbt_valid_from) as min_dbt_valid_from
        from {{ ref('snap_bfs_statatlas_v2') }}
    )

{% endif %}

{% if v1_config is not none %}

    , src_v1 as (
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
    , filter_v1 as (
        select *
        from src_v1
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
    , mapping_v1 as (
        select
        {% if distinct_rows %}
            distinct
        {% endif %}
            {{ indicator_id }} as indicator_id
            , geom_code as geo_code  -- politische gemeinde
            , geo_id::INTEGER as geo_value
            , dbt_valid_from as knowledge_date_from
            {% if v2_config is not none %}
                -- do manually set v1 dbt_valid_to to min of v2
                -- to merge the two snapshot tables together
                , case
                    when dbt_valid_to is NULL then
                        (select min_dbt_valid_from from min_dbt_validity_v2)
                    else
                        dbt_valid_to
                end as knowledge_date_to
            {% else %}
                , dbt_valid_to as knowledge_date_to
            {% endif %}
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
                , NULL::TEXT as group_1_name
                , NULL::TEXT as group_1_value
                , NULL::TEXT as group_2_name
                , NULL::TEXT as group_2_value
                , NULL::TEXT as group_3_name
                , NULL::TEXT as group_3_value
                , NULL::TEXT as group_4_name
                , NULL::TEXT as group_4_value
            {% if value_col_is_text %}
                , NULL::NUMERIC as indicator_value_numeric
                , {{ value_col }}::TEXT as indicator_value_text
            {% else %}
                , {{ value_col }}::NUMERIC as indicator_value_numeric
                , NULL::TEXT as indicator_value_text
            {% endif %}
            , source
            , 1 as _etl_version
        from filter_v1
    )
{% endif %}

{% if v2_config is not none %}

   , src_v2 as (
        select *
        from {{ ref('snap_bfs_statatlas_v2') }}
        where
            bfs_indicateur_id = '{{ v2_indicateur_id }}'
            and (
                bfs_nivgeo like 'polg%'  -- politische gemeinde
                or bfs_nivgeo like 'bezk%'  -- bezirk
                or bfs_nivgeo like 'kant%'  -- kanton
            )
    )

    , keep_only_one_row_v2 as (
        select distinct on (
            -- no bfs_indicateur_id needed, because
            -- per model, only one indicator is selected
            geo_value
            , group_name
            , period_ref
            , dbt_valid_from
        )
            *
        from src_v2
        order by
            geo_value
            , group_name
            , period_ref
            , dbt_valid_from
            -- currently only one supported
            , bfs_nivgeo desc
            , map_id desc
    )

    , mapping_v2 as (
        select
            {{ indicator_id }} as indicator_id
            -- later, geo_code should also be used with a date
            -- every geo_code has its different time dependency
            , left(bfs_nivgeo, 4) as geo_code  -- currently only polg
            , geo_value::INTEGER as geo_value
            , dbt_valid_from as knowledge_date_from
            , dbt_valid_to as knowledge_date_to
            , '{{ period_type }}' as period_type
            , '{{ period_code }}' as period_code
            {% if v2_period_ref_from_transformation %}
                , {{ v2_period_ref_from_transformation }} as period_ref_from
            {% else %}
                , NULL::DATE as period_ref_from
            {% endif %}
            {% if v2_period_ref_transformation %}
                , {{ v2_period_ref_transformation }} as period_ref
            {% else %}
                , period_ref::DATE as period_ref
            {% endif %}
			{% if v2_grouping|length == 1 %}
				{% set group_elem = v2_grouping[0] %}
                , '{{ group_elem.get("name", "") }}' as group_1_name
                , case
                    when group_name = '{{ group_elem.get("total_value", "totvars")}}'
                        then 'GROUP TOTAL'
                    else group_name
                end as group_1_value
            {% else %}
                , NULL::TEXT as group_1_name
                , NULL::TEXT as group_1_value
            {% endif %}
            -- currently not implemented, but one dimensional groups
            -- would be available in STATATLAS_V2, see group_name
            , NULL::TEXT as group_2_name
            , NULL::TEXT as group_2_value
            , NULL::TEXT as group_3_name
            , NULL::TEXT as group_3_value
            , NULL::TEXT as group_4_name
            , NULL::TEXT as group_4_value
            -- value_col_is_text currently not implemented
            , indicator_value::NUMERIC as indicator_value_numeric
            , NULL::TEXT as indicator_value_text
            , source
            , 2 as _etl_version
        from keep_only_one_row_v2
    )

{% endif %}

, union_v1_v2 as (
    {% if v1_config is not none %}
    select *
    from mapping_v1
    {% endif %}
    {% if v1_config is not none and v2_config is not none %}
        union all
    {% endif %}
    {% if v2_config is not none %}
        select *
        from mapping_v2
    {% endif %}
)

, keep_only_one_row_v1_v2 as (
    select distinct on (
        geo_code
        , geo_value
        , period_ref
        , knowledge_date_from
    )
        *
    from union_v1_v2
    order by
        geo_code
        , geo_value
        , period_ref
        , knowledge_date_from
        -- currently only one supported
        -- but should not happen, because the
        -- two snapshot tables are merged and
        -- should never have the same validity
        -- between v1 and v2
        , _etl_version desc
)

, final as (
    select *
    from keep_only_one_row_v1_v2
)
    
{% if execute %}
    {% set measure_config = config.require('odapi').get('measure', none) %}
{% endif %}
{% if measure_config is none %}
    select
        indicator_id
        , geo_code
        , geo_value
        , knowledge_date_from
        , knowledge_date_to
        , period_type
        , period_code
        , period_ref_from
        , period_ref
        , group_1_name
        , group_1_value
        , group_2_name
        , group_2_value
        , group_3_name
        , group_3_value
        , group_4_name
        , group_4_value
        , indicator_value_numeric
        , indicator_value_text
        , source
        , _etl_version
        , 'zahl' as measure_code
    from final
{% endif %}

{% endmacro %}
