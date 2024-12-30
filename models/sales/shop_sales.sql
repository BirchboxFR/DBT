WITH dates AS
(
  SELECT dates
  FROM
  (
  SELECT 
  GENERATE_DATE_ARRAY('2011-01-01', '2100-01-01') AS d
  ) t,
  UNNEST(d) dates
),
history_product_catalog as (

select archive_date,product_id,dw_country_code,max(euro_purchase_price)euro_purchase_price 
from `teamdata-291012.history_table.product__catalog`
group by all
),

c_date AS
(
  SELECT distinct archive_date
  FROM `history_table.product__catalog` 
),
all_dates AS
(
  SELECT dates.dates as d, c_date.archive_date, RANK() OVER (PARTITION BY dates.dates ORDER BY DATE(c_date.archive_date)) AS r
  FROM dates
  LEFT JOIN c_date ON DATE(c_date.archive_date) >= DATE(dates.dates)
  
),
catalog_date AS
(
  SELECT d, archive_date
  FROM all_dates
  WHERE r = 1 
  AND archive_date IS NOT NULL
),
orders_with_box as (
select order_id,dw_country_code 
from `teamdata-291012.inter.order_details`
where product_id=1
group by all

)



SELECT  t.*, 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        unit_price * quantity / (1 + vat_rate / 100) AS gross_revenue,
        (vat_rate / 100) * unit_price * quantity / (1 + vat_rate / 100) AS vat_on_gross_revenue,
        quantity * (unit_product_discount + unit_points_discount + unit_coupon_discount + unit_store_discount + unit_sub_discount) / (1 + vat_rate / 100) AS total_discount,
        (vat_rate / 100) * quantity * (unit_product_discount + unit_points_discount + unit_coupon_discount + unit_store_discount + unit_sub_discount) / (1 + vat_rate / 100) AS vat_on_total_discount,
        unit_product_discount*quantity/(1+vat_rate/100) AS product_discount,
        unit_points_discount*quantity/(1+vat_rate/100) AS points_discount,
        unit_coupon_discount*quantity/(1+vat_rate/100) AS coupons_discount,
        unit_store_discount*quantity/(1+vat_rate/100) AS store_discount,
        unit_sub_discount*quantity/(1+vat_rate/100) AS sub_discount,
        order_total_shipping_ttc/(1+COALESCE(tva.taux,0)/100) AS order_total_shipping,
        COALESCE(tva.taux/100,0)*order_total_shipping_ttc/(1+COALESCE(tva.taux,0)/100) AS vat_on_total_shipping,
        unit_price*quantity/(1+vat_rate/100) - quantity*(unit_product_discount+unit_points_discount+unit_coupon_discount+unit_store_discount+unit_sub_discount)/(1+vat_rate/100) AS net_revenue, 
        quantity * (unit_price - unit_product_discount - unit_coupon_discount - unit_store_discount - unit_sub_discount) AS sell_out
FROM (
    SELECT
        o.dw_country_code,
        o.id AS order_id,
        o.user_id,
        os.value AS order_status,
        COALESCE(o.is_active_sub,0) AS is_active_sub,
        o.is_first_order,
        o.is_first_shop_order,
        DATE(o.date) as order_date,
        d.product_id,
        catalog.sku,
        CASE WHEN d.product_id IN (-1, -2, -3) THEN 'Reward Coupon LOYALTY' ELSE pnn.product_nice_name END AS product_name,
        CASE
            WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN o.dw_country_code
            WHEN o.store_id > 0 THEN 'Store'
        END AS store_code,
        o.store_id,
        CASE 
            WHEN d.special_type = 'DON' THEN 0
            WHEN o.store_id >= 1 AND d.vat = 0 THEN tva.taux
            WHEN o.store_id >= 1 AND d.vat > 0 THEN d.vat
            WHEN d.vat = 0 AND tva.taux IS NOT NULL AND o.date >= '2021-10-01' THEN tva.taux
            WHEN d.vat > 0 THEN d.vat
            WHEN tva.taux IS NULL AND o.date >= '2021-10-01' THEN 0
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
        CASE WHEN o.status_id = 3 THEN 0
             WHEN(pc.category_lvl_1 = 'LOYALTY') THEN 0
             WHEN d.product_id = 41152 THEN 38.0 -- LTE Hygiène intime with wrong price  
             ELSE ROUND(d.price, 2)
        END AS unit_price, -- to fix in code. Loyalty price should be 0 stored
        d.quantity,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.product_discount END AS unit_product_discount,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.coupon_discount END AS unit_coupon_discount,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0.0
             WHEN d.product_id = 41152 AND d.sub_discount > 0 THEN 5.7 -- LTE Hygiène intime with wrong price 
             WHEN d.product_id = 33055 AND d.sub_discount > 0 THEN 1.9 -- La Fabrique with wrong price 
             ELSE ROUND(d.sub_discount,2)
        END AS unit_sub_discount, -- to fix in code. Loyalty price should be 0 stored
        CASE WHEN o.status_id = 3 THEN 0 WHEN d.product_id IN (-1, -2, -3) THEN o.total_points_discount ELSE d.points_discount END AS unit_points_discount, -- to fix in code. Points discount should be ok on order_details
        CASE WHEN o.status_id = 3 THEN 0 WHEN COALESCE(d.store_discount, 0) > d.price THEN d.price*COALESCE(d.store_discount, 0) / 100 ELSE COALESCE(d.store_discount, 0) END AS unit_store_discount,
        b.name AS brand_name,
        p_brand.post_title AS brand_group, 
        COALESCE(b.attr_group_post_id, 0) = 9687 AS is_in_house,
        gc.shipping_mode AS gift_card_type,
        gc.duration AS gift_card_duration,
        COALESCE(coupons_parents.code, coupons.code) AS order_coupon_code,
        CASE WHEN (o.status_id = 3 or owb.order_id is not null) THEN 0
         ELSE o.total_shipping END AS order_total_shipping_ttc,
        sm.name as order_shipping_mode_name,
        sm.id AS shipping_mode_id,
        'detail_validated' as detail_valid,
        COALESCE(hc.euro_purchase_price, catalog.euro_purchase_price) AS euro_purchase_price,
        COALESCE(hc.euro_purchase_price, catalog.euro_purchase_price)*quantity AS products_cost,
        d.bundle_product_id,
        d.bundle_index,
        sm.country as shipping_country,
        p.selections,
        FROM inter.orders o
        INNER JOIN inter.order_details d ON d.order_id = o.ID AND o.dw_country_code = d.dw_country_code
        LEFT JOIN inter.products p ON p.id = d.product_id AND p.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('catalog') }} ON catalog.product_id = p.id AND catalog.dw_country_code = p.dw_country_code
        LEFT JOIN catalog_date cd ON DATE(cd.d) = DATE(o.date)
        LEFT JOIN history_product_catalog hc ON DATE(hc.archive_date) = DATE(cd.archive_date) AND hc.product_id = p.id AND hc.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
        LEFT JOIN inter.product_codification pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
        LEFT JOIN inter.brands b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
        LEFT JOIN bdd_prod_fr.wp_jb_order_status os ON os.id = o.status_id
        LEFT JOIN inter.posts p_brand ON p_brand.ID = b.attr_group_post_id AND p_brand.dw_country_code = b.dw_country_code
        LEFT JOIN inter.shipping_modes sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
        LEFT JOIN inter.tva_product tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
        LEFT JOIN inter.gift_cards gc ON gc.order_detail_id = d.id AND gc.dw_country_code = d.dw_country_code
        LEFT JOIN inter.coupons coupons ON coupons.id = o.coupon_code_id AND coupons.dw_country_code = o.dw_country_code
        LEFT JOIN inter.coupons coupons_parents ON coupons_parents.id = coupons.parent_id AND coupons_parents.dw_country_code = coupons.dw_country_code
        LEFT JOIN orders_with_box owb on owb.order_id=o.id and owb.dw_country_code=o.dw_country_code
        LEFT JOIN inter.da_eu_countries eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
        LEFT JOIN product.codification_bundle_product cbp ON d.dw_country_code = cbp.dw_country_code AND d.product_id = cbp.component_product_id AND d.bundle_product_id = cbp.bundle_product_id
        WHERE o.status_id IN (1, 3, 4)
        AND (p.product_codification_id IN (0, 2, 8, 13, 18, 23, 34, 40, 41, 42, 38, 47) OR p.product_codification_id = 28 AND p.special_type = 'GWP')

    UNION ALL
-- partial cancellations

        SELECT
        o.dw_country_code,
        o.id AS order_id,
        o.user_id,
        os.value AS order_status,
        o.is_active_sub,
        o.is_first_order,
        o.is_first_shop_order,
        DATE(o.date) AS order_date,    
        d.product_id,
        catalog.sku,
        CASE WHEN d.product_id IN(-1, -2, -3) THEN 'Reward Coupon LOYALTY' ELSE pnn.product_nice_name END AS product_name,
        CASE
            WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN o.dw_country_code
            WHEN o.store_id > 0 THEN 'Store'
        END AS store_code,
        o.store_id,
        CASE 
            WHEN o.store_id >= 1 THEN tva.taux
            WHEN tva.taux IS NOT NULL THEN tva.taux
            WHEN eu.country_code IS NULL THEN 0
        ELSE tva.taux
        END AS vat_rate,
        pc.id AS product_codification_id,
        CASE WHEN d.product_id IN (-1, -2, -3) THEN 'LOYALTY COUPON'
             ELSE pc.category_lvl_1
        END AS product_codification,
        apc.category_lvl_1 AS planning_category_1,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0
             ELSE ROUND(d.price, 2)
        END AS unit_price, -- to fix in code. Loyalty price should be 0 stored
        d.quantity,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.product_discount END AS unit_product_discount,
        CASE WHEN o.status_id = 3 THEN 0 ELSE d.coupon_discount END AS unit_coupon_discount,
        CASE WHEN o.status_id = 3 THEN 0
             WHEN pc.category_lvl_1 = 'LOYALTY' THEN 0.0
             ELSE ROUND(d.sub_discount,2)
        END AS unit_sub_discount, -- to fix in code. Loyalty price should be 0 stored
        CASE WHEN o.status_id = 3 THEN 0
             WHEN d.product_id IN (-1, -2, -3) THEN o.total_points_discount
             ELSE d.points_discount
        END AS unit_points_discount, -- to fix in code. Points discount should be ok on order_details
        CASE WHEN o.status_id = 3 THEN 0
             WHEN d.store_discount IS NULL THEN 0
             ELSE d.store_discount
        END AS unit_store_discount,
        b.name AS brand_name,
        p_brand.post_title AS brand_group, 
        COALESCE(b.attr_group_post_id, 0) = 9687 AS is_in_house,
        gc.shipping_mode AS gift_card_type,
        gc.duration AS gift_card_duration,
        COALESCE(coupons_parents.code, coupons.code) AS order_coupon_code,
        CASE WHEN o.status_id = 3 THEN 0 ELSE o.total_shipping END AS order_total_shipping_ttc,
        sm.name AS order_shipping_mode_name,
        sm.id AS shipping_mode_id,
        'detail_cancelled' AS detail_valid,
        COALESCE(hc.euro_purchase_price, catalog.euro_purchase_price) AS euro_purchase_price,
        COALESCE(hc.euro_purchase_price, catalog.euro_purchase_price)*quantity as products_cost,
        NULL AS bundle_product_id,
        NULL AS bundle_index,
        sm.country AS shipping_country,
        p.selections
        FROM inter.orders o
        INNER JOIN inter.partial_cancelations d ON d.order_id = o.ID AND d.dw_country_code = o.dw_country_code
        LEFT JOIN inter.products p ON p.id = d.product_id AND p.dw_country_code = d.dw_country_code
        LEFT JOIN {{ ref('catalog') }} ON catalog.product_id = p.id AND catalog.dw_country_code = p.dw_country_code
        LEFT JOIN catalog_date cd ON DATE(cd.d) = DATE(o.date)
        LEFT JOIN history_product_catalog hc ON DATE(hc.archive_date) = DATE(cd.archive_date) AND hc.product_id = p.id AND hc.dw_country_code = o.dw_country_code
        LEFT JOIN {{ ref('nice_names') }} pnn ON pnn.product_id = p.id AND pnn.dw_country_code = p.dw_country_code
        LEFT JOIN inter.product_codification pc ON pc.id = p.product_codification_id AND pc.dw_country_code = p.dw_country_code
        LEFT JOIN {{ ref('categories') }} apc ON apc.term_id = p.attr_planning_category AND apc.dw_country_code = p.dw_country_code
        LEFT JOIN inter.brands b ON p.brand_id = b.post_id AND p.dw_country_code = b.dw_country_code
        LEFT JOIN bdd_prod_fr.wp_jb_order_status os ON os.id = o.status_id
        LEFT JOIN inter.posts p_brand ON p_brand.ID = b.attr_group_post_id AND p_brand.dw_country_code = b.dw_country_code
        LEFT JOIN inter.shipping_modes sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
        LEFT JOIN inter.gift_cards gc ON gc.order_detail_id = d.order_detail_id AND gc.dw_country_code = d.dw_country_code -- order_detail_id instead of id for wp_jb_order_details
        LEFT JOIN inter.coupons coupons ON coupons.id = o.coupon_code_id AND coupons.dw_country_code = o.dw_country_code
        LEFT JOIN inter.coupons coupons_parents ON coupons_parents.id = coupons.parent_id AND coupons_parents.dw_country_code = coupons.dw_country_code
        LEFT JOIN inter.tva_product tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
        LEFT JOIN inter.da_eu_countries eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
        WHERE o.status_id IN (1, 3, 4)
        AND (p.product_codification_id IN (0, 2, 8, 13, 18, 23, 34, 40, 41, 42, 38, 47) OR p.product_codification_id = 28 AND p.special_type = 'GWP')
    ) t
LEFT JOIN inter.tva_product tva ON tva.country_code = t.shipping_country AND tva.category = 'normal' AND tva.dw_country_code = t.dw_country_code


-- SHOP REFUNDS ---------

UNION ALL

SELECT sr.dw_country_code,
       order_id,
       user_id,
       order_status,
       is_active_sub,
       is_first_order,
       is_first_shop_order,
       order_date,
       sr.product_id,
       sku,
       product_name,
       store_code,
       store_id,
       vat_rate,
       product_codification_id,
       product_codification,
       planning_category_1,
       unit_price,
       quantity,
       unit_product_discount,
       unit_coupon_discount,
       unit_sub_discount,
       unit_points_discount,
       unit_store_discount,
       brand_name,
       brand_group,
       is_in_house,
       gift_card_type,
       gift_card_duration,
       order_coupon_code,
       order_total_shipping_ttc,
       order_shipping_mode_name,
       shipping_mode_id,
       detail_valid,
       euro_purchase_price,
       euro_purchase_price*quantity as products_cost,
       bundle_product_id,
       bundle_index,
       shipping_country,
       sr.selections,
       year,
       month,
       gross_revenue,
       vat_on_gross_revenue,
       total_discount,
       vat_on_total_discount,
       product_discount,
       points_discount,
       coupons_discount,
       store_discount,
       sub_discount,
       order_total_shipping,
       vat_on_total_shipping,
       net_revenue,
       sell_out
FROM sales.shop_refunds sr
left join ( select dw_country_code, sku,product_id from {{ ref('catalog') }})c on c.product_id=sr.product_id and c.dw_country_code=sr.dw_country_code
-- -----------------OTHER SHOP REFUNDS ------------------------------------------------

UNION ALL

SELECT t.dw_country_code,
t.order_id,
t.user_id,
t.order_status,
t.is_active_sub,
t.is_first_order,
t.is_first_shop_order,
t.order_date,
t.product_id,
t.sku,
t.product_name,
t.store_code,
t.store_id,
t.vat_rate,
0 AS product_codification_id,
t.product_codification,
t.planning_category_1,
t.unit_price,
- t.quantity,
t.unit_product_discount,
t.unit_coupon_discount,
t.unit_sub_discount,
t.unit_points_discount,
t.unit_store_discount,
t.brand_name,
t.brand_group,
t.is_in_house,
t.a,
  0,
t.order_coupon_code,
- t.order_total_shipping_ttc,
t.c,
t.shipping_mode,
t.dv,
t.euro_purchase_price,
t.products_cost,
t.bundle_product_id,
t.bundle_index,
t.shipping_country,
'' as selections,
EXTRACT(YEAR FROM t.order_date) AS year,
EXTRACT(MONTH FROM t.order_date) AS month,
- t.total_refunded/(1+vat_rate/100) AS gross_revenue,
- (vat_rate/100)*t.total_refunded/(1+vat_rate/100) AS vat_on_gross_revenue,
0 AS total_discount,
0 AS vat_on_total_discount,
0,
0,
0,
0,
0,
0,
0,
- t.total_refunded/(1+vat_rate/100) AS net_revenue,
- t.total_refunded AS sell_out
FROM
(
    SELECT 
    an.dw_country_code,
    an.order_id,
    o.user_id,
    'refund' AS order_status,
    o.is_active_sub,
    o.is_first_order,
    o.is_first_shop_order,
    DATE(an.created_at) AS order_date,
    0 AS product_id,
    '0' as sku,
    CAST(NULL AS STRING) AS product_name,
    CASE
        WHEN o.store_id = 0 OR o.shipping_mode = 32 THEN an.dw_country_code
        WHEN o.store_id > 0 THEN 'Store'
    END AS store_code,
    o.store_id,
    CASE 
        WHEN o.store_id >= 1 THEN tva.taux
        WHEN tva.taux IS NOT NULL THEN tva.taux
        WHEN eu.country_code IS NULL THEN 0
        ELSE tva.taux
    END AS vat_rate,
    0 AS product_codification_id,
    'ESHOP' AS product_codification,
    CAST(NULL AS STRING) AS planning_category_1,
    0 AS unit_price,
    0 AS quantity,
    0 AS unit_product_discount,
    0 AS unit_coupon_discount,
    0 AS unit_sub_discount,
    0 AS unit_points_discount,
    0 AS unit_store_discount,
    CAST(NULL AS STRING) AS brand_name,
    CAST(NULL AS STRING) AS brand_group,
    False As is_in_house,
    CAST(NULL AS STRING) AS a,
    CAST(NULL AS STRING) AS order_coupon_code,
    False AS b,
    0 AS order_total_shipping_ttc,
    CAST(NULL AS STRING) AS c,
    o.shipping_mode,
    'other refunds' AS dv,
    0 AS euro_purchase_price,
    0 products_cost,
    0 AS bundle_product_id,
    0 AS bundle_index,
    o.shipping_country,
    EXTRACT(YEAR FROM an.created_at) AS year,
    1.0*an.value/100 AS total_refunded
    FROM (SELECT distinct dw_country_code,
                 created_at,
                 order_id,
                 eventCode,
                 sub_id,
                 value
          FROM (
            SELECT dw_country_code,
                   created_at,
                   order_id,
                   eventCode,
                   sub_id,
                   value,
                   ROW_NUMBER() OVER (PARTITION BY dw_country_code, pspReference ORDER BY eventDate DESC) rn
            FROM inter.adyen_notifications
            WHERE success = 1
          )
          WHERE rn = 1) an
    INNER JOIN inter.orders o ON o.id = an.order_id AND o.dw_country_code = an.dw_country_code
    LEFT JOIN sales.shop_refunds AS sr ON sr.order_id = an.order_id AND sr.dw_country_code = an.dw_country_code
    LEFT JOIN sales.box_refunds AS br ON br.sub_id = an.sub_id AND br.dw_country_code = an.dw_country_code
    LEFT JOIN sales.box_refunds AS br2 ON br2.order_id = an.order_id AND br2.dw_country_code = an.dw_country_code
    LEFT JOIN inter.shipping_modes sm ON sm.id = o.shipping_mode AND sm.dw_country_code = o.dw_country_code
    LEFT JOIN inter.tva_product tva ON tva.country_code = sm.country AND tva.category = 'normal' AND tva.dw_country_code = sm.dw_country_code
    LEFT JOIN inter.da_eu_countries eu ON sm.country = eu.country_code AND sm.dw_country_code = eu.dw_country_code
    WHERE an.eventCode IN ('REFUND', 'CANCEL_OR_REFUND')
    AND an.created_at >= '2021-01-01'
    AND sr.order_id IS NULL
    AND br.sub_id IS NULL
    AND br2.order_id IS NULL
) t
