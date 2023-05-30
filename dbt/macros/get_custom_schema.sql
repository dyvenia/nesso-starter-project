-- Makes dbt use the schema as defined in dbt_project.yml, 
-- rather than concatenating target schema (from profiles.yml)
-- in front (<target_schema>_<custom_schema>) -- but only when ran in 'prod' env.
-- See https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}