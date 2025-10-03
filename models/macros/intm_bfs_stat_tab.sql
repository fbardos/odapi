-- Convention: one mart per source?
{% macro intm_bfs_stat_tab(upstream_model) %}
    /* Generates INTM table for STAT-TAB sources.

    */

    -- must be set with a global namespace, will otherwise be reset after exiting each loop

    {% if execute %}
	with src as (
		{% set indicators = config.require('odapi').get('indicators', []) %}
		{% for indicator in indicators %}
			{% set indicator_id = indicator.get('indicator_id', -1) %}
			{% set is_numeric = indicator.get('is_numeric', False) %}
			{% set period_type = indicator.get('period_type', 'duedate') %}
			{% set period_code = indicator.get('period_code', 'year') %}
			{% set grouping = indicator.get('grouping', []) %}
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
				, case
                    {% if period_type == 'period' %}
                        when 1=1 then make_date(year_from, 1, 1)
                    {% else %}
                        when year_from = year_to then NULL::DATE 
                        else make_date(year_from, 1, 1)
                    {% endif %}
                end as period_ref_from  -- can be set to start of year later if correct type is set
				, make_date(year_to, 12, 31) as period_ref
				{% for n in range(1, 5) %}
					{% if grouping|length >= n %}
						{% set group_elem = grouping[n-1] %}
					    {% if group_elem.get('total_value', none) %}
                            , '{{ group_elem.get("name", "") }}'::TEXT as group_{{ n }}_name
                            , CASE
                                WHEN {{ group_elem["column"]}} = '{{ group_elem.get("total_value", "XXX")}}' THEN 'GROUP TOTAL'
                                ELSE {{ group_elem["column"]}}::TEXT
                            END as group_{{ n }}_value
                        {% elif indicator.get('build_total_value', none) %}
                            , '{{ group_elem.get("name", "") }}'::TEXT as group_{{ n }}_name
                            , {{ group_elem["column"]}}::TEXT as group_{{ n }}_value
                        {% else %}
                            , NULL::TEXT as group_{{ n }}_name
                            , NULL::TEXT as group_{{ n }}_value
                        {% endif %}
					{% else %}
						, NULL::TEXT as group_{{ n }}_name
						, NULL::TEXT as group_{{ n }}_value
					{% endif %}
				{% endfor %}
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
	)


    , intm_build_total as (
        {% for indicator in indicators %}
            {% set grouping = indicator.get('grouping', []) %}
            {% if loop.index0 > 0 %}
                UNION ALL
            {% endif %}
            {% if indicator.get('build_total_value', none) %}
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
                    , {{ indicator.get('build_total_value', 'sum(indicator_value_numeric)') }} as indicator_value_numeric
                    , NULL::TEXT as indicator_value_text
                    , source
                from src
                where
                    indicator_id = {{ indicator.get('indicator_id', -1) }}
                group by
                    indicator_id            -- PK INTM
                    , geo_code              -- PK INTM
                    , geo_value             -- PK INTM
                    , knowledge_date_from   -- PK INTM
                    , knowledge_date_to
                    , period_type
                    , period_code
                    , period_ref_from
                    , period_ref            -- PK INTM
                    {% if grouping|length == 1 %}
                        , group_1_name
                        , group_2_name
                        , group_2_value
                        , group_3_name
                        , group_3_value
                        , group_4_name
                        , group_4_value
                        , source
                        , ROLLUP (  -- builds grand total (or other aggregates) for each group
                            group_1_value
                        )
                    {% elif grouping|length == 2 %}
                        , group_1_name
                        , group_2_name
                        , group_3_name
                        , group_3_value
                        , group_4_name
                        , group_4_value
                        , source
                        , ROLLUP (  -- builds grand total (or other aggregates) for each group
                            group_1_value
                            , group_2_value
                        )
                    {% elif grouping|length == 3 %}
                        , group_1_name
                        , group_2_name
                        , group_3_name
                        , group_4_name
                        , group_4_value
                        , source
                        , ROLLUP (  -- builds grand total (or other aggregates) for each group
                            group_1_value
                            , group_2_value
                            , group_3_value
                        )
                    {% elif grouping|length == 4 %}
                        , group_1_name
                        , group_2_name
                        , group_3_name
                        , group_4_name
                        , source
                        , ROLLUP (  -- builds grand total (or other aggregates) for each group
                            group_1_value
                            , group_2_value
                            , group_3_value
                            , group_4_value
                        )
                    {% endif %}
            {% else %}
                select * from src where indicator_id = {{ indicator.get('indicator_id', -1) }}
            {% endif %}
        {% endfor %}
    )

    , set_names_for_group_totals as (
        {% for indicator in indicators %}
            {% set grouping = indicator.get('grouping', []) %}
            {% if loop.index0 > 0 %}
                UNION ALL
            {% endif %}
            {% if indicator.get('build_total_value', none) %}
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
                    {% if grouping|length >= 1 %}
                        , coalesce(group_1_value, 'GROUP TOTAL') as group_1_value
                    {% else %}
                        , NULL::TEXT as group_1_value
                    {% endif %}
                    , group_2_name
                    {% if grouping|length >= 2 %}
                        , coalesce(group_2_value, 'GROUP TOTAL') as group_2_value
                    {% else %}
                        , NULL::TEXT as group_2_value
                    {% endif %}
                    , group_3_name
                    {% if grouping|length >= 3 %}
                        , coalesce(group_3_value, 'GROUP TOTAL') as group_3_value
                    {% else %}
                        , NULL::TEXT as group_3_value
                    {% endif %}
                    , group_4_name
                    {% if grouping|length >= 4 %}
                        , coalesce(group_4_value, 'GROUP TOTAL') as group_4_value
                    {% else %}
                        , NULL::TEXT as group_4_value
                    {% endif %}
                    , indicator_value_numeric
                    , indicator_value_text
                    , source
                from intm_build_total
                where
                    indicator_id = {{ indicator.get('indicator_id', -1) }}
            {% else %}
                select * from intm_build_total where indicator_id = {{ indicator.get('indicator_id', -1) }}
            {% endif %}
        {% endfor %}
    )

    select
        *
        , 1 as _etl_version
    from set_names_for_group_totals
    where
        1=1
        -- global filters
        AND geo_code = 'polg' AND geo_value is not NULL -- can be extended later
        AND (indicator_value_numeric is not NULL or indicator_value_text is not NULL)

    {% endif %}

{% endmacro %}
