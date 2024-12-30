{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Dictionnaire des schémas et leurs tables correspondantes #}
    {%- set schema_mapping = {
        'sales_tmp': ['box_sales'],
        'product_tmp': ['catalog', 'algolia_product_categories', 'categories', 'kit_costs', 'nice_names', 'all_kits'],
        'ops_tmp': ['logistics_costs'],
        'snippets_tmp': ['current_box']
    } -%}

    {# Générer le schéma en vérifiant les noms des tables #}
    {%- for schema, tables in schema_mapping.items() -%}
        {%- if node.name in tables -%}
            {{ schema.strip() }}
            {%- return -%}  {# Terminer dès qu'un schéma correspondant est trouvé #}
        {%- endif -%}
    {%- endfor -%}

    {# Si aucun schéma spécifique trouvé, retourner le schéma par défaut #}
    {{ default_schema.strip() }}
{%- endmacro %}
