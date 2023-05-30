{% macro get_source_pii_columns(dbt_project, schema, table) %}

	{% if execute %}
    
        {% set meta_columns = [] %}
	    {% set fqname = 'source' ~ '.' ~ dbt_project ~ '.' ~ schema ~ '.' ~ table %}
	    {% set columns = graph.sources[fqname]['columns'] %}

        {% for column in columns %}
            {% if 'PII' in graph.sources[fqname]['columns'][column]['tags'] %}
                {% do meta_columns.append(column) %}
            {% endif %}
        {% endfor %}

        {{ return(meta_columns) }}

	{% endif %}

{% endmacro %}
