-- Convention: one mart per source?
{% macro stgn_bfs_stat_tab(src_model) %}
    /* Generates STGN table for STAT-TAB sources.
    
    */

    {% if execute %}
        {% set grouping_columns = config.require('odapi').get('grouping_columns', []) %}

        with src as (
            select
                year::INTEGER
                {% for col in grouping_columns %}
                , {{ col }}::TEXT
                {% endfor %}
                -- handle numeric values
                , case
                    when indicator_value = '"..."' then NULL
                    else indicator_value::NUMERIC
                end as indicator_value
                , NULLIF(geo_code, 'nan') as geo_code
                , geo_value::INTEGER
                , geo_value_name
                , source
                , {{ dbt_utils.generate_surrogate_key(['year'] + grouping_columns + ['geo_code', 'geo_value', 'geo_value_name']) }} as _surr_key
            from {{ src_model }}
        )
        select *
        from src
        where
            -- one of these columns has to be non-empty
            (
                geo_code is not NULL
                OR geo_value is not NULL
                OR geo_value_name is not NULL
            )
            AND indicator_value is not NULL
    
    {% endif %}
{% endmacro %}
