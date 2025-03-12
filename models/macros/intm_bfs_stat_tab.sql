-- Convention: one mart per source?
{% macro intm_bfs_stat_tab(upstream_model) %}
    /* Generates INTM table for STAT-TAB sources.

    */

    {% if execute %}
    {% set indicators = config.require('odapi').get('indicators', []) %}
    {% for indicator in indicators %}
        {% set indicator_id = indicator.get('indicator_id', -1) %}
        {% set is_numeric = indicator.get('is_numeric', False) %}
        {% set period_type = indicator.get('period_type', 'duedate') %}
        {% set period_code = indicator.get('period_code', 'year') %}
        {% if loop.index0 > 0 %}
            UNION ALL
        {% endif %}
        select
            {{ indicator_id }} as indicator_id
            , geo_code
            , geo_value
            , dbt_valid_from as knowledge_date_from
            , dbt_valid_to as knowledge_date_to
            , '{{ period_type }}' as period_type
            , '{{ period_code }}' as period_code
            , NULL::DATE as period_ref_from  -- can be set to start of year later if correct type is set
            , make_date(year, 12, 31) as period_ref
            {% if is_numeric %}
                , indicator_value::NUMERIC as indicator_value_numeric
                , NULL::TEXT as indicator_value_text
            {% else %}
                , NULL::NUMERIC as indicator_value_numeric
                , indicator_value::TEXT as indicator_value_text
            {% endif %}
            , source
        from {{ upstream_model }}
        where
            1=1
            AND geo_code = 'polg'  -- can be extended later
            {% if indicator['filter_and'] %}
                {% for filter_col in indicator.get('filter_and') %}
                    {% if filter_col.get('regex', none) %}
                        AND {{ filter_col['column'] }} ~ '{{ filter_col["regex"] }}'
                    {% endif %}
                    {% if filter_col.get('exact_match', none) %}
                        AND {{ filter_col['column'] }} = '{{ filter_col["exact_match"] }}'
                    {% endif %}
                {% endfor %}
            {% endif %}
    {% endfor %}
    {% endif %}
{% endmacro %}
