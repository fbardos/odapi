{% macro intm_get_upstream_config() %}
  {% set current_model_name = model.name %}

  {# strip off everything after the first "__" #}
  {% set base_model_name = current_model_name.split('__')[0] %}

  {# look up the base model node in the graph #}
  {% set node = graph.nodes.get(ref(base_model_name).unique_id) %}

  {% set candidates = [] %}
  {% for n in graph.nodes.values() %}
    {% if n.resource_type == 'model' and n.name == base_model_name %}
      {% do candidates.append(n) %}
    {% endif %}
  {% endfor %}
  {% if candidates | length == 1 %}
    {% set node = candidates[0] %}
  {% else %}
    {% do exceptions.raise_compiler_error("intm_get_upstream_config: Could not find upstream model '" ~ base ~ "'.") %}
  {% endif %}

  {# return its meta config (or empty dict if missing) #}
  {{ print(node) }}
  {{ return(node.config or {}) }}
{% endmacro %}
