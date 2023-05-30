{# Create a nice Markdown template for a dbt model description. #}

{% macro create_description_markdown(relation_name=none, docs_name=none, schema=none) %}

{% if docs_name is none %}
  {% set docs_name = schema + "_" + relation_name %}
{% endif %}

{% if execute %}
  {{ print('{% docs ' + docs_name + '  %}') }}
  {{ print('## `' + relation_name + '` table') }}
  
  {{ print("") }}

  {{ print("### 📝 Details") }}
  {{ print("-") }}

  {{ print("") }}

  {{ print('### 📚 External docs') }}
  {{ print("-") }}
  {{ print('{% enddocs %}') }}
{%- endif -%}
{%- endmacro -%}