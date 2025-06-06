
WITH last_adyen_notifications AS (
  SELECT dw_country_code,
         created_at,
         order_id,
         eventCode,
         eventDate,
         sub_id,
         value
  FROM (
    SELECT dw_country_code,
           created_at,
           order_id,
           eventCode,
           eventDate,
           sub_id,
           value,
           ROW_NUMBER() OVER (PARTITION BY dw_country_code, pspReference ORDER BY eventDate DESC) rn
    FROM {{ ref('adyen_notifications') }}
    WHERE success = 1
  )
  WHERE rn = 1
)

-- -------- total refunds ------------
SELECT  t.*, 
        EXTRACT(year FROM order_date) AS year,
        EXTRACT(month FROM order_date) AS month,
        unit_price*quantity/(1+vat_rate/100) AS gross_revenue,
        (vat_rate/100)*unit_price*quantity/(1+vat_rate/100) AS vat_on_gross_revenue,
        quantity*(unit_product_discount+unit_points_discount+unit_coupon_discount+unit_store_discount+unit_sub_discount)/(1+vat_rate/100) AS total_discount,
        (vat_rate/100)*quantity*(unit_product_discount+unit_points_discount+unit_coupon_discount+unit_store_discount+unit_sub_discount)/(1+vat_rate/100) AS vat_on_total_discount,
        unit_product_discount*quantity/(1+vat_rate/100) AS product_discount,
        unit_points_discount*quantity/(1+vat_rate/100) AS points_discount,
        unit_coupon_discount*quantity/(1+vat_rate/100) AS coupons_discount,
        unit_store_discount*quantity/(1+vat_rate/100) AS store_discount,
        unit_sub_discount*quantity/(1+vat_rate/100) AS sub_discount,
        - order_total_shipping_ttc/(1 + COALESCE(tva.taux, 0)/100) AS order_total_shipping,
        - (COALESCE(tva.taux, 0)/100)*order_total_shipping_ttc/(1 + COALESCE(tva.taux, 0)/100) AS vat_on_total_shipping,
        unit_price*quantity/(1+vat_rate/100) - quantity*(unit_product_discount+unit_points_discount+unit_coupon_discount+unit_store_discount+unit_sub_discount)/(1+vat_rate/100) AS net_revenue,
        quantity * (unit_price - unit_product_discount - unit_coupon_discount - unit_store_discount - unit_sub_discount) AS sell_out
FROM (
    SELECT 
        an.dw_country_code,
        o.id AS order_id,
        o.user_id,
        'refund' AS order_status,
        COALESCE(o.is_active_sub, 0) AS is_active_sub,
        o.is_first_order,
        o.is_first_shop_order,
        DATE(an.created_at) AS order_date, 
        d.product_id,
        CASE WHEN d.product_id IN(-1, -2, -3) THEN 'Reward Coupon LOYALTY' ELSE pnn.product_nice_name END AS product_name,
        CASE
            WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN o.dw_country_code
            WHEN o.store_id >= 1 THEN 'Store'
        END AS store_code,
        o.store_id,
        CASE 
            WHEN d.special_type = 'DON' THEN 0
            WHEN o.store_id >= 1 AND d.vat = 0 THEN tva.taux
            WHEN o.store_id >= 1 AND d.vat > 0 THEN d.vat
            WHEN d.vat = 0 AND tva.taux IS NOT NULL AND o.date >= '2021-10-01' THEN tva.taux
            WHEN d.vat > 0 THEN d.vat
            WHEN eu.country_code IS NULL THEN 0
            ELSE tva.taux
        END AS vat_rate,
        CASE WHEN cbp.product_codification_id IS NOT NULL THEN cbp.product_codification_id
             ELSE pc.id
        END AS product_codification_id,
        CASE WHEN cbp.product_codification_id IS NOT NULL THEN cbp.product_codification
             WHEN d.product_id IN (-1, -2, -3) THEN 'LOYALTY COUPON' 
             ELSE pc.category_lvl_1
        END AS product_codification,
        apc.category_lvl_1 AS planning_category_1,
        apc.category_lvl_2 AS planning_category_2,
         apc.category_lvl_3 AS planning_category_3,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN(pc.category_lvl_1 = 'LOYALTY') THEN 0
             WHEN d.dw_country_code = 'FR' AND d.product_id = 41152 THEN 38.0 -- LTE Hygiène intime with wrong price  
             WHEN d.dw_country_code = 'FR' AND d.product_id = 33055 THEN 16.9 -- LTE Hygiène intime with wrong price  
             ELSE ROUND(d.price, 2)
        END AS unit_price, -- to fix in code. Loyalty price should be 0 stored
        - d.quantity AS quantity,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.product_discount END AS unit_product_discount,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.coupon_discount END AS unit_coupon_discount,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0.0
             WHEN d.dw_country_code = 'FR' AND d.product_id = 41152 AND d.sub_discount > 0 THEN 5.7 -- LTE Hygiène intime with wrong price 
             WHEN d.dw_country_code = 'FR' AND d.product_id = 33055 AND d.sub_discount > 0 THEN 1.9 -- La Fabrique with wrong price 
             ELSE ROUND(d.sub_discount, 2)
        END AS unit_sub_discount, -- to fix in code. Loyalty price should be 0 stored
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.product_id IN(-1, -2, -3) THEN o.total_points_discount ELSE d.points_discount END AS unit_points_discount, -- to fix in code. Points discount should be ok on order_details
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.price * (CASE WHEN d.store_discount IS NULL THEN 0 ELSE d.store_discount END)/100 END AS unit_store_discount,
        b.name AS brand_name,
        p_brand.post_title AS brand_group, 
        COALESCE(b.attr_group_post_id, 0) = 9687 AS is_in_house,
        gc.shipping_mode AS gift_card_type,
        gc.duration AS gift_card_duration, 
        COALESCE(coupons_parents.code, coupons.code) AS order_coupon_code,
        CASE WHEN o.status_id = 3 THEN 0 ELSE o.total_shipping END AS order_total_shipping_ttc,
        sm.name AS order_shipping_mode_name,
        sm.id AS shipping_mode_id,
        'detail_validated' AS detail_valid,
        catalog.euro_purchase_price,
        d.bundle_product_id,
        d.bundle_index,
        sm.country AS shipping_country,
        p.selections
        FROM last_adyen_notifications an
        INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
        INNER JOIN {{ ref('order_details') }} d ON d.order_id = o.ID AND d.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('products') }} p ON p.id = d.product_id AND p.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('catalog') }} catalog ON catalog.product_id = p.id AND catalog.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('product_codification') }} pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('brands') }} b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('order_status') }} os ON os.id = o.status_id AND os.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('posts') }} p_brand ON p_brand.ID = b.attr_group_post_id AND p_brand.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('shipping_modes') }} sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('tva_product') }} tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
        LEFT JOIN {{ ref('gift_cards') }} gc ON gc.order_detail_id = d.id AND gc.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('coupons') }} coupons ON coupons.id = o.coupon_code_id AND coupons.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('coupons') }} coupons_parents ON coupons_parents.id = coupons.parent_id AND coupons_parents.dw_country_code = coupons.dw_country_code
        LEFT JOIN {{ ref('da_eu_countries') }} eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
        LEFT JOIN {{ ref('codification_bundle_product') }} cbp ON d.dw_country_code = cbp.dw_country_code AND d.product_id = cbp.component_product_id AND d.bundle_product_id = cbp.bundle_product_id
        WHERE an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
        AND (an.sub_id =0 OR an.sub_id IS NULL)
        AND abs(1.0*an.value/100 - o.total) <= 0.03
        AND an.created_at >= '2021-05-01'
        AND p.product_codification_id IN (0, 2, 8, 13, 18, 23, 34, 38, 40, 41, 42, 47) 

    UNION ALL
-- partial cancellations

        SELECT
        an.dw_country_code,
        o.id AS order_id,
        o.user_id,
        'refund' AS order_status,
        o.is_active_sub,
        o.is_first_order,
        o.is_first_shop_order,
       DATE(an.created_at) AS order_date,    
        d.product_id,
        CASE WHEN d.product_id IN (-1, -2, -3) THEN 'Reward Coupon LOYALTY' ELSE pnn.product_nice_name END AS product_name,
        CASE
            WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN o.dw_country_code
            WHEN o.store_id = 1 THEN 'Store'
        END AS store_code,
        o.store_id,
        CASE 
            WHEN d.special_type = 'DON' THEN 0
            WHEN o.store_id = 1 THEN tva.taux
            WHEN tva.taux IS NOT NULL AND o.date >= '2021-10-01' THEN tva.taux
            WHEN eu.country_code IS NULL THEN 0
            ELSE tva.taux
        END AS vat_rate,
        pc.id AS product_codification_id,
        CASE WHEN d.product_id IN(-1, -2, -3) THEN 'LOYALTY COUPON'
             ELSE pc.category_lvl_1
        END AS product_codification,
        apc.category_lvl_1 AS planning_category_1,
        apc.category_lvl_2 AS planning_category_2,
         apc.category_lvl_3 AS planning_category_3,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN(pc.category_lvl_1 = 'LOYALTY') THEN 0
             ELSE ROUND(d.price,2)
        END AS unit_price, -- to fix in code. Loyalty price should be 0 stored
        - d.quantity AS quantity,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.product_discount END AS unit_product_discount,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.coupon_discount END AS unit_coupon_discount,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0.0
             ELSE ROUND(d.sub_discount, 2)
        END AS unit_sub_discount, -- to fix in code. Loyalty price should be 0 stored
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.product_id IN(-1, -2, -3) THEN o.total_points_discount ELSE d.points_discount END AS unit_points_discount, -- to fix in code. Points discount should be ok on order_details
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.store_discount IS NULL THEN 0 ELSE d.store_discount END AS unit_store_discount,
        b.name AS brand_name,
        p_brand.post_title AS brand_group, 
        COALESCE(b.attr_group_post_id, 0) = 9687 AS is_in_house,
        gc.shipping_mode AS gift_card_type,
        gc.duration AS gift_card_duration, 
        COALESCE(coupons_parents.code, coupons.code) AS order_coupon_code,
        CASE WHEN o.status_id = 3 THEN o.total_shipping END AS order_total_shipping_ttc,
        sm.name AS order_shipping_mode_name,
        sm.id AS shipping_mode_id,
        'detail_cancelled' AS detail_valid,
        catalog.euro_purchase_price,
        NULL AS bundle_product_id,
        NULL AS bundle_index,
        sm.country AS shipping_country,
        p.selections
        FROM last_adyen_notifications an
        INNER JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
        INNER JOIN {{ ref('partial_cancelations') }} d ON d.order_id = o.ID AND d.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('products') }} p ON p.id = d.product_id AND p.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('catalog') }} catalog ON catalog.product_id = p.id AND catalog.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('product_codification') }} pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('brands') }} b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('order_status') }} os ON os.id = o.status_id AND os.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('posts') }} p_brand ON p_brand.ID = b.attr_group_post_id AND p_brand.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('shipping_modes') }} sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('tva_product') }} tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
        LEFT JOIN {{ ref('gift_cards') }} gc ON gc.order_detail_id = d.order_detail_id AND gc.dw_country_code = d.dw_country_code -- order_detail_id instead of id for wp_jb_order_details
        LEFT JOIN {{ ref('coupons') }} coupons ON coupons.id = o.coupon_code_id AND coupons.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('coupons') }} coupons_parents ON coupons_parents.id = coupons.parent_id AND coupons_parents.dw_country_code = an.dw_country_code
        LEFT JOIN {{ ref('da_eu_countries') }} eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
        WHERE an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
        AND (an.sub_id =0 OR an.sub_id IS NULL)
        AND abs(1.0*an.value/100 - o.total) <= 0.03
        AND an.created_at >= '2021-05-01'
        AND p.product_codification_id IN (0, 2, 8, 13, 18, 23, 34, 38, 40, 41, 42, 47) 

  UNION ALL

  -- ------------------------- PARTIAL REFUNDS -------------------------------
  
  SELECT
        o.dw_country_code,
        o.id AS order_id,
        o.user_id,
        'refund' AS order_status,
        o.is_active_sub,
        o.is_first_order,
        o.is_first_shop_order,
        DATE(TIMESTAMP(d.date), "Europe/Paris") AS order_date,        
        d.product_id,
        CASE WHEN d.product_id IN (-1, -2, -3) THEN 'Reward Coupon LOYALTY' ELSE pnn.product_nice_name END AS product_name,
        CASE
            WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN o.dw_country_code
            WHEN o.store_id >= 1 THEN 'Store'
        END AS store_code,
        o.store_id, 
        CASE 
            WHEN d.special_type = 'DON' THEN 0
            WHEN o.store_id = 1 THEN tva.taux
            WHEN tva.taux IS NOT NULL AND o.date >= '2021-10-01' THEN tva.taux
            WHEN eu.country_code IS NULL THEN 0
            ELSE 20
        END AS vat_rate,
        pc.id AS product_codification_id,
        CASE WHEN d.product_id IN (-1, -2, -3) THEN 'LOYALTY COUPON'
             ELSE pc.category_lvl_1
        END AS product_codification,
        apc.category_lvl_1 AS planning_category_1,
        apc.category_lvl_2 AS planning_category_2,
         apc.category_lvl_3 AS planning_category_3,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN(pc.category_lvl_1 = 'LOYALTY') THEN 0
             ELSE ROUND(d.price,2)
        END AS unit_price, -- to fix in code. Loyalty price should be 0 stored
        - d.quantity AS quantity,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.product_discount END AS unit_product_discount,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.coupon_discount END AS unit_coupon_discount,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0.0
             ELSE ROUND(d.sub_discount, 2)
        END AS unit_sub_discount, -- to fix in code. Loyalty price should be 0 stored
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.product_id IN(-1, -2, -3) THEN o.total_points_discount ELSE d.points_discount END AS unit_points_discount, -- to fix in code. Points discount should be ok on order_details
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.store_discount IS NULL THEN 0 ELSE d.store_discount END AS unit_store_discount,
        b.name AS brand_name,
        p_brand.post_title AS brand_group, 
        COALESCE(b.attr_group_post_id, 0) = 9687 AS is_in_house,
        gc.shipping_mode AS gift_card_type, 
        gc.duration AS gift_card_duration, 
        COALESCE(coupons_parents.code, coupons.code) AS order_coupon_code,
        CASE WHEN o.status_id = 3 THEN o.total_shipping END AS order_total_shipping_ttc,
        sm.name AS order_shipping_mode_name,
        sm.id AS shipping_mode_id,
        'detail_cancelled' AS detail_valid,
        catalog.euro_purchase_price,
        NULL AS bundle_product_id,
        NULL AS bundle_index,
        sm.country AS shipping_country,
        p.selections
        FROM {{ ref('orders') }} o
        INNER JOIN {{ ref('partial_cancelations') }} d ON d.order_id = o.ID AND o.dw_country_code = d.dw_country_code
        INNER JOIN
              (
                  SELECT an.dw_country_code,
                         an.order_id,
                         FORMAT_DATE('%Y-%m-%d', an.created_at) AS d,
                         SUM(1.0*an.value/100) AS adyen_refunds
                  FROM last_adyen_notifications an
                  LEFT JOIN {{ ref('orders') }} o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
                  LEFT JOIN {{ ref('order_details') }} dbox ON dbox.order_id = o.id AND dbox.product_id = 1 AND dbox.dw_country_code = o.dw_country_code
                  LEFT JOIN {{ ref('order_detail_sub') }} sbox ON sbox.order_detail_id = dbox.id AND sbox.box_id = dbox.sub_start_box AND sbox.dw_country_code = dbox.dw_country_code
                  WHERE an.eventDate>= '2021-05-01'
                  AND an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
                  AND (an.sub_id =0 OR an.sub_id IS NULL)
                  AND ABS(1.0*an.value/100 - o.total) > 0.03
                  AND ABS(1.0*an.value/100 - CASE WHEN sbox.total IS NULL THEN 0.0 ELSE sbox.total END) > 0.02
                  GROUP BY an.dw_country_code,
                           an.order_id,
                           d
              ) partial_refunds ON partial_refunds.order_id = d.order_id AND partial_refunds.d = FORMAT_DATE('%Y-%m-%d', d.date) AND partial_refunds.dw_country_code = d.dw_country_code AND partial_refunds.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('products') }} p ON p.id = d.product_id AND p.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('catalog') }} ON catalog.product_id = p.id AND catalog.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('product_codification') }} pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('brands') }} b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('order_status') }} os ON os.id = o.status_id AND os.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('posts') }} p_brand ON p_brand.ID = b.attr_group_post_id AND p_brand.dw_country_code = b.dw_country_code
        LEFT JOIN {{ ref('shipping_modes') }} sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('tva_product') }} tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
        LEFT JOIN {{ ref('gift_cards') }} gc ON gc.order_detail_id = d.order_detail_id AND gc.dw_country_code = d.dw_country_code -- order_detail_id instead of id for wp_jb_order_details
        LEFT JOIN {{ ref('coupons') }} coupons ON coupons.id = o.coupon_code_id AND coupons.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('coupons') }} coupons_parents ON coupons_parents.id = coupons.parent_id AND coupons_parents.dw_country_code = coupons.dw_country_code
        LEFT JOIN {{ ref('da_eu_countries') }} eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
        WHERE o.status_id IN (1, 3, 4)
        AND p.product_codification_id IN (0, 2, 8, 13, 18, 23, 34, 38, 40, 41, 42, 47)
 
    ) t
LEFT JOIN {{ ref('tva_product') }} tva ON tva.country_code = t.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = t.dw_country_code
