{% macro hash(field) -%}
  {{ return(adapter.dispatch('hash', 'dbt') (field)) }}
{%- endmacro %}

{% macro default__hash(field) -%}
    md5(cast({{ field }} as {{ api.Column.translate_type('string') }}))
{%- endmacro %}

{% macro databricks__hash(field) -%}
    sha2(cast({{ field }} as {{ api.Column.translate_type('string') }}), 256)
{%- endmacro %}


{% macro hash_column(column) %}
{{hash(column)}}
{% endmacro %}