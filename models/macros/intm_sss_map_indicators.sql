-- Convention: one mart per source?
{% macro intm_sss_map_indicators(upstream_cte) %}
    /*

    Maps indicators to columns in INTM table.

    All INTM columns must be present in the source table.

    */

    {% if execute %}
        {% set indicators = config.require('odapi').get('indicators', []) %}
    {% endif %}

    {% for col in indicators %}
        {% if loop.index0 > 0 %}
            UNION ALL
        {% endif %}
        select
            {{ col['indicator_id'] }} as indicator_id
            , geo_code
            , geo_value
            , knowledge_date_from
            , knowledge_date_to
            , period_type
            , period_code
            , period_ref_from
            , period_ref
            , {{ col['column_name'] }} as indicator_value_numeric
            , NULL::TEXT as indicator_value_text
            , source
        from {{ upstream_cte }}
    {% endfor %}
{% endmacro %}
