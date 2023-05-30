{# Adapted from dbt-codegen. #}
{# Generate seed schema and add seeds into it. #}

{% macro generate_seed_schema_yaml() %}

    {% set yaml=[] %}
    {% do yaml.append('version: 2') %}
    {% do yaml.append('') %}
    {% do yaml.append('seeds: []') %}

    {% if execute %}
        {% set joined = yaml | join ('\n') %}
        {{ print(joined) }}
        {% do return(joined) %}
    {% endif %}

{% endmacro %}


{% macro generate_seed_yaml(
    seed,
    database_name=target.database,
    schema_name=target.schema,
    generate_columns=True,
    include_tags=False,
    include_owners=True,
    technical_owner="",
    business_owner="",
    domains=[],
    true_source=[],
    case_sensitive_cols=True
    ) %}

    {% set yaml=[] %}

    {% do yaml.append('  - name: ' ~ seed | lower ) %}
    {% do yaml.append('    description: ""' ) %}

    {% if include_tags %}
        {% do yaml.append('    tags: []' ) %}
    {% endif %}

    {% if include_owners %}
        {% do yaml.append('    meta:' ) %}
        {% do yaml.append('      technical_owner: ' ~ technical_owner)%}
        {% do yaml.append('      business_owner: ' ~ business_owner)%}
        {% do yaml.append('      domains: ' ~ domains)%}
        {% do yaml.append('      true_source: ' ~ true_source)%}
    {% endif %}

    {% if generate_columns %}
        {% do yaml.append('    columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=seed
        ) %}
        {% set columns = adapter.get_columns_in_relation(table_relation) %}
        {% for column in columns %}
            {% if case_sensitive_cols %}
                {% do yaml.append('      - name: ' ~ column.name) %}
                {% do yaml.append('        quote: True') %}
            {% else %}
                {% do yaml.append('      - name: ' ~ column.name | lower ) %}
            {% endif %}
            {% do yaml.append('        description: ""' ) %}
            {% do yaml.append('        tests:' ) %}
            {% do yaml.append('          # - unique' ) %}
            {% do yaml.append('          # - not_null' ) %}
            {% do yaml.append('          # - accepted_values:' ) %}
            {% do yaml.append('          #   values: ["value1", "value2"]' ) %}
        {% endfor %}

    {% endif %}

    {% if execute %}
        {% set joined = yaml | join ('\n') %}
        {{ print(joined) }}
        {% do return(joined) %}
    {% endif %}

{% endmacro %}
