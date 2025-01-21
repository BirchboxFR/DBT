{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Listes des tables spécifiques pour chaque schéma #}
    {%- set sales_tmp_tables = ['box_sales','box_refunds','box_gift','kpi_box','box_sales_by_user_by_type','box_acquisition_daily','shop_sales','box_acquisition_detail','shop_orders_margin','shop_refunds','obj_by_country'] -%}
    {%- set ops_tmp_tables = ['logistics_costs','shipping_costs','box_shipments','shop_shipments','shipments_all'] -%}
    {%- set snippets_tmp_tables = ['current_box'] -%}
    {%- set allocation_tmp_tables = ['index_user_samples','box_choose'] -%}
    {%- set payment_tmp_tables = ['adyen_notifications_authorization'] -%}
    {%- set product_tmp_tables = ['catalog', 'algolia_product_categories', 'categories','kit_costs','all_kits','nice_names','codification_bundle_product','stock','stock_bs','stock_store','kit_details','reviews','product_classes','product_classes_3m'] -%}
    {%- set marketing_tmp_tables = ['live_expenses'] -%}
    {%- set accounting_tmp_tables = ['reconciliation_live','shop_detailed','box_turnover','box_detailed'] -%}
    {%- set blissim_analytics_tmp_tables = ['monthly_rank_brands'] -%}
    {%- set inter_tmp_tables = ['choose_users','products','users','tags','comments','products_stock_log','product_warehouse_location','kit_links','products_bundle_component','posts','adyen_notifications','wp_jb_products_stock_log','orders','order_details','order_detail_sub','sub_offers','coupons','sub_order_link','gift_cards','sub_history','sub_suspend_survey_result','sub_suspend_survey_result_answer','inventory_items','partial_cancelations'] -%}
    {%- set inter_view_tmp_tables = ['inter_tmp.boxes', 'inter_tmp.brands', 'inter_tmp.brands_correspondances', 'inter_tmp.business_objectives', 'inter_tmp.byob_product_link', 'inter_tmp.choose_choices', 'inter_tmp.choose_forms', 'inter_tmp.company', 'inter_tmp.da_box_acquisition_detail', 'inter_tmp.da_box_shipped_detail', 'inter_tmp.da_eu_countries', 'inter_tmp.da_monthly_sub_baseline', 'inter_tmp.expected_inbound_details', 'inter_tmp.expected_inbounds', 'inter_tmp.invoice_credit_notes', 'inter_tmp.invoices', 'inter_tmp.invoice_details', 'inter_tmp.lte_kits', 'inter_tmp.mini_byob_reexp', 'inter_tmp.mini_lte_reexp', 'inter_tmp.open_comment_posts', 'inter_tmp.order_detail_sub_options', 'inter_tmp.order_status', 'inter_tmp.partial_box_paid', 'inter_tmp.prepacked_products', 'inter_tmp.product_codification', 'inter_tmp.purchase_orders', 'inter_tmp.purchase_order_items', 'inter_tmp.raf_offer_details', 'inter_tmp.raf_offers', 'inter_tmp.raf_reward_moment', 'inter_tmp.raf_reward_type', 'inter_tmp.raf_sub_link', 'inter_tmp.range_of_age', 'inter_tmp.sample_product_link', 'inter_tmp.shipping_modes', 'inter_tmp.store_products', 'inter_tmp.sub_payments_status', 'inter_tmp.sub_suspend_survey_question', 'inter_tmp.sub_suspend_survey_question_answer', 'inter_tmp.sub_suspended_reasons', 'inter_tmp.survey_answer_meanings', 'inter_tmp.survey_question_categories', 'inter_tmp.term_taxonomy', 'inter_tmp.terms'] -%}  
    {%- set user_tmp_tables = ['segments','today_whales','today_stars'] -%}

    {%- if node.resource_type == "test" -%}
        dbt_test_failures  {# Schéma dédié pour les résultats des tests #}
    {%- elif node.name in sales_tmp_tables -%}
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
    {%- elif node.name in inter_views_tmp_tables -%}
        inter_tmp
    {%- elif node.name in user_tmp_tables -%}
        user
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}