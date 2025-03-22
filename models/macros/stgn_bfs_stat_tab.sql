{% macro stgn_bfs_stat_tab(src_model) %}
    /* Generates STGN table for STAT-TAB sources.

        Returns CTE 'final' (leaves room for further transformations)

    */

    {% if execute %}
        {% set grouping_columns = config.require('odapi').get('grouping_columns', []) %}

        with src as (
            select
                year_from::INTEGER
                , year_to::INTEGER
                {% for col in grouping_columns %}
                    , {{ col }}::TEXT
                {% endfor %}
                -- handle numeric values
                , case
                    when indicator_value = '"..."' then NULL
                    when indicator_value = '"...."' then NULL
                    when indicator_value = '"......"' then NULL
                    when indicator_value = '' then NULL
                    else indicator_value::NUMERIC
                end as indicator_value
                , NULLIF(geo_code, '') as geo_code
                , geo_value::INTEGER
                , geo_value_name
                , source
                , {{ dbt_utils.generate_surrogate_key(['year_from', 'year_to'] + grouping_columns + ['geo_code', 'geo_value', 'geo_value_name']) }} as _surr_key
            from {{ src_model }}
        )
        -- needed for table with no gemeinde_bfs_id
        , join_gemeinde_by_name as (
            select distinct on (_surr_key)
                src.year_from
                , src.year_to
                {% for col in grouping_columns %}
                    , src.{{ col }}::TEXT
                {% endfor %}
                , src.indicator_value
                , src.geo_code
                , coalesce(src.geo_value, gem.gemeinde_bfs_id) as geo_value
                , coalesce(src.geo_value_name, gemeinde_name) as geo_value_name
                , src.source
                , src._surr_key
            from src
                left join {{ ref('dim_gemeinde_latest') }} gem on
                    src.geo_code = 'polg'
                    and src.geo_value_name = gem.gemeinde_name
            order by _surr_key, gem.gemeinde_bfs_id desc
        )
        , final as (
            select *
            from join_gemeinde_by_name
            where
                -- one of these columns has to be non-empty
                (
                    geo_value is not NULL
                    OR geo_value_name is not NULL
                )
                AND indicator_value is not NULL
        )
    {% endif %}
{% endmacro %}
