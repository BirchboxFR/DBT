{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Listes des tables spécifiques pour chaque schéma #}
    {%- set sales_tmp_tables = ['box_sales','box_refunds','box_gift','kpi_box','box_sales_by_user_by_type','box_acquisition_daily','shop_sales','box_acquisition_detail','shop_orders_margin','shop_refunds'] -%}
    {%- set ops_tmp_tables = ['logistics_costs','shipping_costs'] -%}
    {%- set snippets_tmp_tables = ['current_box'] -%}
    {%- set allocation_tmp_tables = ['index_user_samples','box_choose'] -%}
    {%- set payment_tmp_tables = ['adyen_notifications_authorization'] -%}
    {%- set product_tmp_tables = ['catalog', 'algolia_product_categories', 'categories','kit_costs','all_kits','nice_names','codification_bundle_product','stock','stock_bs','stock_store','kit_details','reviews','product_classes','product_classes_3m'] -%}
    {%- set marketing_tmp_tables = ['live_expenses'] -%}
    {%- set accounting_tmp_tables = ['reconciliation_live','shop_detailed','box_turnover','box_detailed'] -%}
    {%- set blissim_analytics_tmp_tables = ['monthly_rank_brands'] -%}

    {%- set inter_tmp_tables = ['choose_users','products','users','tags','comments','products_stock_log','product_warehouse_location','kit_links','products_bundle_component','posts','adyen_notifications','wp_jb_products_stock_log','orders','order_details','order_detail_sub','sub_offers','coupons','sub_order_link','gift_cards','sub_history','sub_suspend_survey_result','sub_suspend_survey_result_answer','inventory_items','partial_cancelations'] -%}

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
        paymenttmp
    {%- elif node.name in marketing_tmp_tables -%}
        marketing_tmp
    {%- elif node.name in accounting_tmp_tables -%}
        accounting_tmp
    {%- elif node.name in blissim_analytics_tmp_tables -%}
        blissim_analytics_tmp
    {%- elif node.name in inter_tmp_tables -%}
        inter_tmp
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}