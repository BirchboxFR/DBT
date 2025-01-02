{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Dictionnaire des schémas et leurs tables correspondantes #}
    {%- set schema_mapping = {
        'sales_tmp': ['box_sales'],
        'product_tmp': ['catalog', 'algolia_product_categories', 'categories', 'kit_costs', 'nice_names', 'all_kits'],
        'ops_tmp': ['logistics_costs'],
        'snippets_tmp': ['current_box']
    } -%}

    {# Initialiser une variable pour stocker le schéma sélectionné #}
    {%- set selected_schema = default_schema -%}

    {# Vérifier si le nom du modèle correspond à une liste de tables #}
    {%- for schema, tables in schema_mapping.items() -%}
        {%- if node.name in tables -%}
            {%- set selected_schema = schema -%}
            {%- break -%} {# Sortir de la boucle après avoir trouvé une correspondance #}
        {%- endif -%}
    {%- endfor -%}

    {# Retourner le schéma sélectionné #}
    {{ selected_schema }}
{%- endmacro %}
