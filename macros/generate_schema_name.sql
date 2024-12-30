{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Dictionnaire des schémas et leurs tables correspondantes #}
    {%- set schema_mapping = {
        'sales_tmp': ['box_sales','box_refunds'],
        'product_tmp': ['catalog', 'algolia_product_categories', 'categories', 'kit_costs', 'nice_names', 'all_kits'],
        'ops_tmp': ['logistics_costs'],
        'snippets_tmp': ['current_box']
    } -%}

    {# Trouver le schéma correspondant #}
    {%- for schema, tables in schema_mapping.items() -%}
        {%- if node.name in tables -%}
            {{ schema }}
        {%- break -%}
    {%- else -%}
        {{ default_schema }}
    {%- endfor -%}
{%- endmacro %}
