{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Listes des tables spécifiques pour chaque schéma #}
    {%- set sales_tmp_tables = ['box_sales'] -%}
    {%- set product_tmp_tables = ['catalog', 'algolia_product_categories', 'categories','kit_costs','nice_names'] -%}

    {%- if node.name in sales_tmp_tables -%}
        sales_tmp
    {%- elif node.name in product_tmp_tables -%}
        product_tmp
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}