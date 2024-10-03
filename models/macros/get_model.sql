-- Gets config for the selected model
-- Source: https://stackoverflow.com/a/77514105/4856719
{% macro get_model(relation) -%}
    {% for node in graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", relation.identifier) %}
        {% do return(node) %}
    {% endfor %}
{%- endmacro %}
