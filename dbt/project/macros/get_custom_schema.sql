{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the custom schema (silver / gold) exactly as given, instead of
        dbt's default "<target_schema>_<custom_schema>" concatenation, so
        models land in demo.silver / demo.gold to match the bronze layer
        written by the Spark Structured Streaming job. -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
