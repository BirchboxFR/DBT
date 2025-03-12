{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}

    {# Listes des tables spécifiques pour chaque schéma #}
    {%- set sales_tmp_tables = ['box_mono','box_sales','box_refunds','box_gift','kpi_box','box_sales_by_user_by_type','box_acquisition_daily','shop_sales','box_acquisition_detail','shop_orders_margin','shop_refunds','obj_by_country','shop_sales_with_gross_profit'] -%}
    {%- set ops_tmp_tables = ['logistics_costs','shipping_costs','box_shipments','shop_shipments','shipments_all'] -%}
    {%- set snippets_tmp_tables = ['current_box'] -%}
    {%- set allocation_tmp_tables = ['index_user_samples','box_choose'] -%}
    {%- set payment_tmp_tables = ['adyen_notifications_authorization'] -%}
    {%- set product_tmp_tables = ['catalog', 'algolia_product_categories', 'categories','kit_costs','all_kits','nice_names','codification_bundle_product','stock','stock_bs','stock_store','kit_details','reviews','product_classes','product_classes_3m'] -%}
    {%- set marketing_tmp_tables = ['live_expenses','Marketing_cac_live','Marketing_cac_expenses','Marketing_cac_budget_vs_expenses'] -%}
    {%- set accounting_tmp_tables = ['reconciliation_live','shop_detailed','box_turnover','box_detailed'] -%}
    {%- set blissim_analytics_tmp_tables = ['monthly_rank_brands'] -%}
    {%- set sublissim_tmp_tables = ['kit'] -%}
    {%- set inter_tmp_tables = ['order_detail_sub_merged','order_details_merged','survey_surveys','choose_users','products','users','tags','comments','products_stock_log','product_warehouse_location','kit_links','products_bundle_component','posts','adyen_notifications','products_stock_log','orders','order_details','order_detail_sub','sub_offers','coupons','sub_order_link','gift_cards','sub_history','sub_suspend_survey_result','sub_suspend_survey_result_answer','inventory_items','partial_cancelations'] -%}
    {%- set inter_view_tmp_tables = ['christmas_offer','b2c_order_notifications','cc_orders_status','boxes','boxes_by_day', 'brands', 'brands_correspondances', 'business_objectives', 'byob_product_link', 'choose_choices', 'choose_forms', 'company', 'da_box_acquisition_detail', 'da_box_shipped_detail', 'da_eu_countries', 'da_monthly_sub_baseline', 'expected_inbound_details', 'expected_inbounds', 'invoice_credit_notes', 'invoices', 'invoice_details', 'lte_kits', 'mini_byob_reexp', 'mini_lte_reexp', 'open_comment_posts', 'order_detail_sub_options', 'order_status', 'partial_box_paid', 'prepacked_products', 'product_codification', 'purchase_orders', 'purchase_order_items', 'raf_offer_details', 'raf_offers', 'raf_reward_moment', 'raf_reward_type', 'raf_sub_link', 'range_of_age', 'sample_product_link', 'shipping_modes', 'store_products', 'sub_payments_status', 'sub_suspend_survey_question', 'sub_suspend_survey_question_answer', 'sub_suspended_reasons', 'survey_answer_meanings', 'survey_question_categories', 'term_taxonomy', 'terms'] -%}  
    {%- set user_tmp_tables = ['splio_data_dedup','customers','today_whales','today_stars','today_spectators','today_inactive','today_lost','today_middle','today_new','today_prospects','today_risky','today_spectators','today_segments'] -%}
    {%- set inter_materialized_view_tmp_tables = ['warehouse','user_consent','tva_product','shipup_tracking','survey_questions','survey_results','survey_Result_answers','consent','b2c_order_notifications','orders_status','consent_topic','gift_codes_generated','options','order_status','payments','postmeta','survey_answers','survey_questions','survey_result_answers','survey_results','user_consent_history','allocation_history','b2c_exported_orders','ga_transactions','mini_reexp','optin','payment_profiles','raf','raf_order_link','reception_details','reward_points_history','reward_points_history_uses','saved_cart','saved_cart_details','store_mouvements','term_relationships','trackings','user_campaign','user_mailing_list'] -%}
    {%- set forecast_tmp_tables = ['classement_groupe_marque'] -%}
    {%- set alerting_tmp_tables = ['surveillance_incremental'] -%}
    

    {%- set inter_all_tables = inter_tmp_tables + inter_view_tmp_tables + inter_materialized_view_tmp_tables   -%}


    {%- if node.resource_type == "test" -%}
        dbt_test_failures  {# Schéma dédié pour les résultats des tests #}
    {%- elif node.name in sales_tmp_tables -%}
        sales
    {%- elif node.name in product_tmp_tables -%}
        product
    {%- elif node.name in ops_tmp_tables -%}
        ops
    {%- elif node.name in snippets_tmp_tables -%}
        snippets
    {%- elif node.name in allocation_tmp_tables -%}
        allocation
    {%- elif node.name in payment_tmp_tables -%}
        payment
    {%- elif node.name in marketing_tmp_tables -%}
        marketing
    {%- elif node.name in accounting_tmp_tables -%}
        accounting
    {%- elif node.name in blissim_analytics_tmp_tables -%}
        blissim_analytics
    {%- elif node.name in inter_all_tables -%}
        inter
    {%- elif node.name in sublissim_tmp_tables -%}
        bdd_prod_sublissim
    {%- elif node.name in alerting_tmp_tables -%}
        alerting
    {%- elif node.name in forecast_tmp_tables -%}
        forecast
    {%- elif node.name in user_tmp_tables -%}
        user
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}