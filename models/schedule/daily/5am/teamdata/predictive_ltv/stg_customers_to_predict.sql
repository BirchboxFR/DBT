{{
    config(
        materialized='table'
    )
}}

WITH 
customers_to_predict AS (
    SELECT DISTINCT bs.user_id
    FROM sales.box_sales as bs
    LEFT JOIN predictive_ltv.ltv l 
        ON l.user_id = bs.user_id 
        AND l.dw_country_code = bs.dw_country_code
    WHERE bs.dw_country_code = 'FR'
        AND l.user_id IS NULL
        AND bs.payment_date < DATE_ADD(CURRENT_DATE, INTERVAL -3 DAY)
),

first_order AS (
    SELECT
        user_id,
        MIN(date) AS cohorte_client
    FROM sales.box_sales
    WHERE net_revenue > 0
        AND dw_country_code = 'FR'
        AND payment_status = 'paid'
    GROUP BY user_id    
),

box_sales_with_cohort AS (
    SELECT
        first_order.cohorte_client,
        sales.date,
        sales.user_id AS user_id_sales,
        COALESCE(sales.gross_profit, 0) AS gross_profit_box_sales,
        NULL AS gross_profit_shop_sales
    FROM sales.box_sales sales
    LEFT JOIN first_order 
        ON sales.user_id = first_order.user_id
    WHERE net_revenue > 0
        AND sales.dw_country_code = 'FR'
        AND payment_status = 'paid'
),

shop_sales_with_cohort AS (
    SELECT
        first_order.cohorte_client,
        shop_sales.order_date AS date,
        shop_sales.user_id AS user_id_sales,
        NULL AS gross_profit_box_sales,
        COALESCE(shop_sales.net_revenue, 0) - COALESCE(shop_sales.products_cost, 0) AS gross_profit_shop_sales
    FROM sales.shop_orders_margin shop_sales
    LEFT JOIN first_order 
        ON shop_sales.user_id = first_order.user_id
    WHERE shop_sales.dw_country_code = 'FR'
),

combined_sales AS (
    SELECT * FROM box_sales_with_cohort
    UNION ALL
    SELECT * FROM shop_sales_with_cohort
),

aggreg AS (
    SELECT
        user_id_sales,
        cohorte_client,
        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 0 AND 2 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_3_mois, 

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 3 AND 5 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_6_mois, 

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 6 AND 11 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_12_mois, 

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 12 AND 23 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_24_mois,

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 24 AND 35 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_36_mois,

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 36 AND 47 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_48_mois,

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 48 AND 59 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_60_mois,

        SUM(COALESCE(CASE WHEN DATE_DIFF(DATE(date), DATE(cohorte_client), MONTH) BETWEEN 0 AND 5000 THEN COALESCE(gross_profit_box_sales, 0) + COALESCE(gross_profit_shop_sales, 0) ELSE 0 END, 0)) AS gross_profit_period_total_mois
    FROM combined_sales 
    WHERE cohorte_client IS NOT NULL
    GROUP BY user_id_sales, cohorte_client
    ORDER BY user_id_sales, cohorte_client
),

customers_pre_filtered AS (
    SELECT * 
    FROM user.customers
    WHERE dw_country_code = 'FR'
), 

final_table AS (
    SELECT 
        aggreg.*, 
        cpf.birth_date, 
        cpf.optin_email, 
        cpf.optin as optin_box,
        cpf.optin as optin_news,
        cpf.optin as optin_spl,
        cpf.optin as optin_deals, 
        cpf.optin_sms, 
        cpf.ltm_has_seen_box_page,
        cpf.ltm_has_seen_product_fullsize_page,
        cpf.ltm_has_seen_search_page,
        cpf.ltm_has_seen_checkout_page,
        cpf.open_email, 
        cpf.click, 
        cpf.ltm_open_email_rate,  
        cpf.skin_complexion, 
        cpf.skin_type,
        cpf.skin_redness,
        cpf.skin_sensitiveness,
        cpf.skin_aging,
        cpf.skin_acne,
        cpf.face_care,
        cpf.body_care,
        cpf.bath_products, 
        cpf.makeup_general,
        cpf.makeup_eyes,
        cpf.makeup_lips,
        cpf.makeup_eyebrows,
        cpf.makeup_complexion,
        cpf.makeup_nails,
        cpf.hair_shampoo,
        cpf.hair_conditioner,
        cpf.hair_mask,
        cpf.hair_styling,
        cpf.accessories,
        cpf.food_supplements,
        cpf.green_natural_products,
        cpf.slimming_products,
        cpf.perfumes,
        cpf.self_taining,
        cpf.solid_cosmetics,
        cpf.hair_products,
        cpf.initial_sub_type, 
        cpf.initial_is_committed, 
        cpf.initial_coupon_code, 
        cpf.hair_dye,
        cpf.hair_scalp,
        cpf.hair_damaged,
        cpf.beauty_routine,
        cpf.beauty_budget,
        cpf.shop_perfumery,
        cpf.shop_brand_store,
        cpf.shop_hairdressing,
        cpf.shop_pharmacy,
        cpf.shop_hypermarket,
        cpf.shop_bio_store,
        cpf.shop_internet,
        cpf.is_ever_gifted,
        cpf.first_order_type,
        cpf.initial_coupon_type
    FROM aggreg
    LEFT JOIN customers_pre_filtered cpf
        ON aggreg.user_id_sales = cpf.user_id
)

SELECT 
    user_id_sales as user_id,
    cohorte_client,
    birth_date, 
    optin_email, 
    optin_box, 
    optin_news,
    optin_spl,
    optin_deals, 
    optin_sms, 
    ltm_has_seen_box_page,
    ltm_has_seen_product_fullsize_page,
    ltm_has_seen_search_page,
    ltm_has_seen_checkout_page,
    open_email, 
    click, 
    ltm_open_email_rate,  
    skin_complexion, 
    skin_type, 
    skin_redness,
    skin_sensitiveness,
    skin_aging,
    skin_acne,
    face_care,
    body_care,
    bath_products,
    makeup_general,
    makeup_eyes,
    makeup_lips,
    makeup_eyebrows,
    makeup_complexion,
    makeup_nails,
    hair_shampoo,
    hair_conditioner,
    hair_mask,
    hair_styling,
    accessories,
    food_supplements,
    green_natural_products,
    slimming_products,
    perfumes,
    self_taining,
    solid_cosmetics,
    hair_products,
    initial_sub_type, 
    initial_is_committed,
    initial_coupon_code, 
    hair_dye,
    hair_scalp,
    hair_damaged,
    beauty_routine,
    beauty_budget, 
    shop_perfumery,
    shop_brand_store,
    shop_hairdressing,
    shop_pharmacy,
    shop_hypermarket,
    shop_bio_store,
    shop_internet,
    is_ever_gifted,
    first_order_type,
    initial_coupon_type
FROM final_table
JOIN customers_to_predict cp 
    ON cp.user_id = final_table.user_id_sales