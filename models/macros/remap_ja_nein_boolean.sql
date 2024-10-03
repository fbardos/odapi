{% macro remap_ja_nein_boolean(column_name) %}
case
    when {{ column_name }} = 'Ja' then true
    when {{ column_name }} = 'Nein' then false
    else null
end as {{ column_name }}
{% endmacro %}
