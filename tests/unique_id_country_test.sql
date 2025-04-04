
{% set models_to_test = [
'adyen_notifications', 'allocation_history', 'b2c_exported_orders', 'b2c_order_notifications', 'boxes', 'boxes_by_day', 'brands', 'brands_correspondances', 'choose_choices', 'choose_forms', 'choose_users', 'christmas_offer', 'company', 'consent', 'coupons', 'expected_inbound_details', 'expected_inbounds', 'ga_transactions', 'gift_cards', 'gift_codes_generated', 'inventory_items', 'invoice_credit_notes', 'invoice_details', 'invoices', 'kit_links', 'lte_kits', 'mini_byob_reexp', 'mini_lte_reexp', 'mini_reexp', 'open_comment_posts', 'optin', 'order_detail_sub', 'order_detail_sub_options', 'order_details', 'order_status', 'orders', 'partial_box_paid', 'partial_cancelations', 'payment_profiles', 'payments', 'posts', 'prepacked_products', 'product_codification', 'product_warehouse_location', 'products', 'products_bundle_component', 'products_stock_log', 'purchase_order_items', 'purchase_orders', 'raf', 'raf_offer_details', 'raf_offers', 'raf_order_link', 'raf_reward_moment', 'raf_reward_type', 'raf_sub_link', 'range_of_age', 'reception_details', 'reward_points_history', 'reward_points_history_uses', 'sample_product_link', 'shipping_modes', 'shipup_tracking', 'store_mouvements', 'store_products', 'sub_history', 'sub_offers', 'sub_order_link', 'sub_payments_status', 'survey_answer_meanings', 'survey_answers', 'survey_question_categories', 'survey_questions', 'survey_result_answers', 'survey_results', 'survey_surveys', 'tags', 'user_campaign', 'user_consent', 'user_consent_history', 'users', 'warehouse'
] %}

{% for model_name in models_to_test %}
    {% if not loop.first %}
        union all
    {% endif %}
    
    select
        '{{ model_name }}' as model_name,
        id,
        dw_country_code,
        count(*) as records_count
    from {{ ref(model_name) }}
    where id is not null
      and dw_country_code is not null
    group by id, dw_country_code
    having count(*) > 1
{% endfor %}