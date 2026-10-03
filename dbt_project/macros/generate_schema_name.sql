{# Override anti-concatenation : utilise +schema tel quel (staging, intermediate,
   marts) au lieu du defaut dbt <target.schema>_<custom_schema>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
