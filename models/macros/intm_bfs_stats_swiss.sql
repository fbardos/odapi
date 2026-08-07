-- Convention: one mart per source?
{% macro intm_bfs_stats_swiss(upstream_model) %}
    /* Generates INTM table for stats.swiss sources.

    */

    {% if execute %}
	with src as (

		{% set global_config = config.require('odapi') %}
		{% set geo_config = global_config.get('geo_config') %}
        {% set geo_code = geo_config.get('base_code', 'polg') %}
        {% set geo_value_regex = geo_config.get('base_value_regex', None) %}
        {% set geo_value_type = geo_config.get('base_value_type', None) %}
        {% set geo_grouping_key = geo_config.get('base_grouping_key', 'GEMEINDENAME') %}
		
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
                -- indicator_id
				{{ indicator_id }} as indicator_id
                -- geo_code
				{% if geo_code %}
                    , '{{ geo_code }}' as geo_code
				{% else %}
					, 'polg' as geo_code
				{% endif %}
                -- geo_value
				{% if geo_value_type %}
                    {% if geo_value_regex %}
                        , case
                            when k.key ~ '{{ geo_value_regex }}' then k.key::{{ geo_value_type }}
                            else NULL
                        end
                    {% else %}
                        , k.key::{{ geo_value_type }}
                    {% endif %}
				{% else %}
					, k.key
				{% endif %}
                as geo_value
                -- knowledge_date
				, dbt_valid_from as knowledge_date_from
				, dbt_valid_to as knowledge_date_to
                -- period
				, '{{ period_type }}' as period_type
				, '{{ period_code }}' as period_code
				, 
                    {% if period_type == 'period' %}
                        make_date(time_period, 1, 1)
                    {% else %}
                        NULL::DATE 
                    {% endif %}
                as period_ref_from  -- can be set to start of year later if correct type is set
				, make_date(time_period, 12, 31) as period_ref
                -- grouping
                ,
				{% set group_len = grouping|length %}
                {% for n in range(group_len) %}
                    jsonb_set(
                {% endfor %}
                    grouping,
				{% for group_config in grouping %}
				    {% set group_filter_stat = "{" ~ group_config.get('column') ~ ',' ~ group_config.get('total_value') ~ "}" %}
                    '{{ group_filter_stat }}',
                    '"GROUP TOTAL"',
                    false
                    {% if loop.last %}
                        )
                    {% else %}
                        ),
                    {% endif %}
				{% endfor %}
                - '{{ geo_grouping_key }}' as grouping
                -- indicator_value
				{% if is_numeric %}
					, obs_value::NUMERIC as indicator_value_numeric
					, NULL::TEXT as indicator_value_text
				{% else %}
					, NULL::NUMERIC as indicator_value_numeric
					, obs_value::TEXT as indicator_value_text
                {% endif %}
				, source
			from {{ upstream_model }} t
                cross join lateral (
                      select jsonb_object_keys(t.grouping->'{{ geo_grouping_key }}') AS key
                      limit 1
                ) k
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


    -- TODO: intm_build_total, see macro intm_bfs_stat_tab
    -- TODO: set_names_for_group_totals, see macro intm_bfs_stat_tab

    select
        *
        , 1 as _etl_version
    from src
    where
        1=1
        -- global filters
        -- AND geo_code = 'polg' AND geo_value is not NULL -- can be extended later
        AND geo_value is not null
        AND (indicator_value_numeric is not NULL or indicator_value_text is not NULL)

    {% endif %}

{% endmacro %}
