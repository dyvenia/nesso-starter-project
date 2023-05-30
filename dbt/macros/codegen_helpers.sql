{# retrieve models directly upstream from a given model #}
{% macro get_model_dependencies(model_name) %}
    {{ log("Getting upstream dependencies for model '" ~ model_name ~ "'...") }}
    
    {% set upstream_fqns=[] %}

    {{ log("Checking upstream models...") }}
    {% for node in graph.nodes.values() | selectattr('name', "equalto", model_name) %}
        {% if node.depends_on.nodes and not "source." in node.depends_on.nodes[0] %}
            {# The node depends on another model. #}
            {{ log("Got the following dependencies: " ~ node.depends_on.nodes ~ ".") }}
            {{ return({"type": "model", "nodes": node.depends_on.nodes}) }}
        {% endif %}
    {% endfor %}

    {{ log("Checking upstream source...") }}
    {% for node in graph.sources.values() | selectattr('name', "equalto", model_name) %}
        {{ return({"type": "source", "node": node.unique_id}) }}
    {% endfor %}

{% endmacro %}


{% macro get_source_or_model_column_metadata(model_name, model_type = "model") %}
{# 
Get column metadata (description and tags) for a model or source.

Returns: Dict[str, Dict[str, Any]]

Example:
>>> dbt run-operation get_source_or_model_column_metadata --args '{"model_name": "c4c_contact", "model_type": "model"}'
>>> {"id": {"description": "A", "tags": []}}
#}
    
    {% if model_type == "model" %}
        {% set nodes = graph.nodes.values() %}
    {% else %}
        {% set nodes = graph.sources.values() %}
    {% endif %}

    {% set columns_metadata_dict = {} %}
    {% for node in nodes | selectattr('name', "equalto", model_name) %}
        {% for col_name, col_values in node.columns.items() %}
            {% do columns_metadata_dict.update(
                {col_name: {
                    "description": col_values.description,
                    "tags": col_values.tags
                    }}
                ) %}
        {% endfor %}
    {% endfor %}

    {{ return(columns_metadata_dict) }}

{% endmacro %}

{# build a global dictionary looping through all the direct parents models #}

{% macro get_parent_source_or_model_column_metadata(model_name) %}
{# 
Get column metadata (description and tags) for the model's or source's
parent source or model.

This is useful for automatically populating YAML files of downstream models
with the information already provided in upstream (for example, if a view
uses a field from a source amd this field's description is already available 
in the source's YAML file).

Note that if the same column name exists in multiple upstream models, 
the description will be overwritten at each loop and the final one
will be taken from the model that happens to be the last in the loop. 

Returns: Dict[str, Dict[str, Any]]

Example:
>>> dbt run-operation get_parent_source_or_model_column_metadata --args '{"model_name": "c4c_contact"}'
>>> {"id": {"description": "B", "tags": []}}
#}

    {{ log("Getting column-level metadata for " ~ model_type ~ " '" ~ model_name ~ "'...") }}

    {% if execute %}
        {% set dependencies = get_model_dependencies(model_name) %}
        {% set model_type = dependencies["type"] %}

        {# Note we immediately return `column_metadata`, as outside the if/else, it's magically set to None. #}
        {% if model_type == "model" %}
            {% for full_model in dependencies["nodes"] %}
                {% set upstream_model_name = full_model.split('.')[-1] %}
                {% set column_metadata = get_source_or_model_column_metadata(model_name=upstream_model_name, model_type=model_type) %}
            {{ return(column_metadata) }}
            {% endfor %}
        {% endif %}

        {% if model_type == "source" %}
            {% set upstream_model_name = dependencies["node"].split('.')[-1] %}
            {% set column_metadata = get_source_or_model_column_metadata(model_name=upstream_model_name, model_type=model_type) %}
            {{ return(column_metadata) }}
        {% endif %}
    
    {% endif %}
{% endmacro %}


{% macro get_source_or_model_metadata(model_name, model_type = "model") %}
{# 
Get table metadata (description, tags, and meta) for a model or source.

Note that if there are multiple upstream models, the metadata will
be overwritten at each loop and the final one will be taken from the model 
that happens to be the last in the loop. 

Returns: Dict[str, Union[str, List[str], Dict[str, Any]]]

Example:
>>> dbt run-operation get_source_or_model_metadata --args '{"model_name": "c4c_contact", "model_type": "model"}'
>>> {"description": "A", "tags": [], "meta": {"owner": js@example.com}}
#}
    
    {{ log("Getting model-level metadata for " ~ model_type ~ " '" ~ model_name ~ "'...") }}

    {% if model_type == "model" %}
        {% set nodes = graph.nodes.values() %}
    {% else %}
        {% set nodes = graph.sources.values() %}
    {% endif %}

    {% set table_metadata_dict = {} %}
    {% for node in nodes | selectattr('name', "equalto", model_name) %}
        {% do table_metadata_dict.update(
            {
                "description": node.description,
                "tags": node.tags,
                "meta": node.meta 
            }
        ) %}
    {% endfor %}

    {{ log("Successfully retrieved model-level metadata for " ~ model_type ~ " '" ~ model_name ~ "'.") }}

    {{ return(table_metadata_dict) }}
{% endmacro %}


{% macro get_parent_source_or_model_metadata(model_name) %}
{# 
Get table metadata (description, tags, and meta) for the model's parent source or model.

This is useful for automatically populating YAML files of downstream models
with the information already provided in upstream (eg. when defining
base views).

Returns: Dict[str, Union[str, List[str], Dict[str, Any]]]

Example:
>>> dbt run-operation get_parent_source_or_model_metadata --args '{"model_name": "c4c_contact"}'
>>> {"description": "B", "tags": [], "meta": {"owner": js@example.com}}
#}
    {% if execute %}

        {{ log("") }}
        {{ log("Running `get_parent_source_or_model_metadata()`...") }}

        {% set dependencies = get_model_dependencies(model_name) %}
        {% set model_type = dependencies["type"] %}
        
        {# Note we immediately return `model_metadata`, as outside the if/else, it's magically set to None. #}
        {% if model_type == "model" %}
            {% for full_model in dependencies["nodes"] %}
                {% set model_name = full_model.split('.')[-1] %}
                {% set model_metadata = get_source_or_model_metadata(model_name, model_type=model_type) %}
            {% do return(model_metadata) %}
            {% endfor %}
        {% elif model_type == "source" %}
            {% set model_name = dependencies["node"].split('.')[-1] %}
            {% set model_metadata = get_source_or_model_metadata(model_name, model_type=model_type) %}
            {% do return(model_metadata) %}
        {% else %}
            {{ log("Incorrect model type (" ~ model_type ~ ").") }}
        {% endif %}

        {{ log("Finishing `get_parent_source_or_model_metadata()...") }}
        {{ log("") }}

    {% endif %}
{% endmacro %}