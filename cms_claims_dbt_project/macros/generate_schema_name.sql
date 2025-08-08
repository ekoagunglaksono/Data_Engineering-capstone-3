{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      custom_schema_name = nilai dari +schema di dbt_project.yml atau di config model
      target.schema      = dataset default dari profiles.yml
    #}
    {% if custom_schema_name is not none %}
        {{ return(custom_schema_name) }}
    {% else %}
        {{ return(target.schema) }}
    {% endif %}
{%- endmacro %}
