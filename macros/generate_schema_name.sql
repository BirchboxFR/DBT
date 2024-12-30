{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Listes des tables spécifiques pour chaque schéma #}
    {%- set sales_tmp_tables = ['box_sales','box_refunds','box_gift'] -%}
    {%- set ops_tmp_tables = ['logistics_costs','shipping_costs'] -%}
    {%- set snippets_tmp_tables = ['current_box'] -%}
    {%- set allocation_tmp_tables = ['index_user_samples'] -%}
    {%- set payment_tmp_tables = ['adyen_notifications_authorization'] -%}
    {%- set product_tmp_tables = ['catalog', 'algolia_product_categories', 'categories','kit_costs','nice_names','codification_bundle_product','stock','stock_bs','stock_store','kit_details'] -%}
     {%- set marketing_tmp_tables = ['live_expenses'] -%}

    {%- if node.name in sales_tmp_tables -%}
        sales_tmp
    {%- elif node.name in product_tmp_tables -%}
        product_tmp
    {%- elif node.name in ops_tmp_tables -%}
        ops_tmp
    {%- elif node.name in snippets_tmp_tables -%}
        snippets_tmp
    {%- elif node.name in allocation_tmp_tables -%}
        allocation_tmp
    {%- elif node.name in payment_tmp_tables -%}
        payment_tmp
    {%- elif node.name in marketing_tmp_tables -%}
        marketing_tmp
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}