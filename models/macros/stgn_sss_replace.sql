{% macro stgn_sss_replace(indicator_column_list) %}
    {% for indicator in indicator_column_list %}
        , case
            -- replace placeholder for empty cells
            when src.{{ indicator }}::TEXT in ('...', '…', '–', '-') then 0
            -- replace empty cells with a footnote, example of a footnot: –2)
            when src.{{ indicator }}::TEXT ~ '–\d+(\.\d+)?\)' then NULL::NUMERIC
            -- replace special "–" with a "-" to be parsed as negative number
            when src.{{ indicator }}::TEXT ~ '–\d+' then REGEXP_REPLACE(src.{{ indicator }}::TEXT, '–', '-')::NUMERIC
            else src.{{ indicator }}::NUMERIC
        end as {{ indicator }}
    {% endfor %}
{% endmacro %}
