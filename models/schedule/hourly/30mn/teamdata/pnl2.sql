WITH 

-- For work orders repartition 
sales as 
(
  SELECT bs.dw_country_code, CAST(bs.date AS STRING) AS sku, 'BOX' AS product_codification, count(*) as qty
  FROM {{ ref('box_sales') }} bs
  WHERE 1=1
  AND bs.payment_status = 'paid'
  AND bs.box_id >= 112
  GROUP BY bs.dw_country_code, bs.date

  UNION ALL

  SELECT ss.dw_country_code, c.sku, ss.product_codification, SUM(ss.quantity) AS qty 
  FROM {{ ref('shop_sales') }} as ss
  JOIN {{ ref('catalog') }} c ON c.product_id = ss.product_id AND c.dw_country_code = ss.dw_country_code
  WHERE ss.product_codification IN ('LTE', 'SPLENDIST', 'CALENDAR')
  AND ss.order_date >= '2021-01-01'
  GROUP BY ss.dw_country_code, ss.product_codification, c.sku
),

total_sales AS
(
  SELECT sku, sales.product_codification, SUM(qty) AS qty
  FROM sales 
  GROUP BY sku, sales.product_codification
),

box_produced as 
( 
  SELECT EXTRACT ( year  FROM p.start_date ) as year , 
  EXTRACT ( month FROM p.start_date) as month, 
  p.project_type as product_codification,
  CASE WHEN p.project_type = 'BOX' THEN CAST(start_date AS STRING) ELSE wo.kit_sku END as sku,
    SUM(wo.produced_quantity) AS produced_quantity
  FROM catalog.work_orders_materialized wo
  JOIN catalog.projects_materialized p ON p.project_full_name = wo.project_full_name
  GROUP BY p.start_date, p.project_type, sku
),
repart_qty AS
(
  SELECT bp.year, bp.month, bp.sku, COALESCE(sales.dw_country_code, 'FR') AS country,bp.product_codification, bp.produced_quantity, sales.qty, total_sales.qty,
  ROUND(bp.produced_quantity* SAFE_DIVIDE(COALESCE(sales.qty,1), COALESCE(total_sales.qty,1)),0) AS qty_repart
  FROM box_produced bp
  LEFT JOIN sales ON sales.sku = bp.sku
  LEFT JOIN total_sales ON total_sales.sku = sales.sku
)

-- sales online
(SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, 'GROSS REVENUE' as type, product_codification,  SUM(gross_revenue) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1 
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- sales online - quantities
(SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, LOWER(CONCAT('qty-',ss.dw_country_code,'-',product_codification)) as type, product_codification,  SUM(quantity) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 --  AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_status <> 'refund'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- cash Giftcards sold
SELECT ss.dw_country_code, ss.year, ss.month, ss.store_code AS store, LOWER(CONCAT('cash-ht-',ss.dw_country_code,'-',product_codification)) as type, product_codification,  SUM(net_revenue) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1 
AND product_codification = 'GIFT' 
AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification, ss.store_code

UNION ALL

SELECT ss.dw_country_code, ss.year, ss.month, ss.store_code AS store, LOWER(CONCAT('cash-ttc-',ss.dw_country_code,'-',product_codification)) as type, product_codification,  SUM(net_revenue*(1+vat_rate/100)) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1 
AND product_codification = 'GIFT'
AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type,
 ss.year, ss.month, product_codification, ss.store_code

UNION ALL

-- sales online - nb orders
(SELECT ss.dw_country_code, ss.year, ss.month, 'METRIC' AS store, LOWER(CONCAT('orders-',ss.dw_country_code,'-',product_codification)) as type, product_codification,  count(distinct ss.order_id) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND (  
   product_codification NOT IN ('GIFT', 'LOYALTY COUPON') 
   OR (product_codification = 'GIFT' AND gift_card_type = 'PHYSICAL') 
     )
AND ss.store_code <> 'Store'
AND ss.order_status <> 'refund'
AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- nb orders for shipping and  cost
SELECT dw_country_code,
       year,
       month, 
       'ONLINE',
       'COGS LOG NB ORDERS',
       CASE WHEN codification = 0 THEN 'ESHOP'
            WHEN codification = 1 THEN 'BYOB'
            WHEN codification = 2 THEN 'LTE'
            WHEN codification = 8 THEN 'SPLENDIST'
            WHEN codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       count(*) AS nb_orders
FROM {{ ref('shop_orders_margin') }}
GROUP BY dw_country_code, year, month, product_codification


UNION ALL

-- sales store
(SELECT ss.dw_country_code, ss.year, ss.month, 'Store Paris' AS store, 'GROSS REVENUE' as type, 'ALL',   SUM(gross_revenue) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code = 'Store'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month)

UNION ALL

--all discounts online
(SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, 'DISCOUNT' as type, product_codification,  SUM(total_discount) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- points discount online
SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, 'POINTS DISCOUNT' as type, product_codification,  SUM(points_discount) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification

UNION ALL

-- all discounts Store
(SELECT ss.dw_country_code, ss.year, ss.month, 'Store Paris' AS store, 'DISCOUNT' as type, 'ALL',   SUM(total_discount) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code = 'Store'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month)

UNION ALL

-- shipping shop
(
  SELECT dw_country_code, year, month, 'ONLINE' AS store, 'SHIPPING SHOP' as type, 'ALL', SUM(order_total_shipping) as value
  FROM (
 SELECT ss.dw_country_code, ss.year, ss.month, order_id, order_total_shipping
  FROM {{ ref('shop_sales') }} ss
  WHERE 1=1
  AND ss.order_date >= '2021-01-01'
  GROUP BY  ss.dw_country_code, ss.year, ss.month, order_id, order_total_shipping
) t
  GROUP BY dw_country_code, year, month
)

UNION ALL

-- shipping shop -- NEW repartition pas utilisée)
(
  SELECT dw_country_code, year, month, 'ONLINE' AS store, 'SHIPPING SHOP' as type, 
  CASE WHEN codification = 0 THEN 'ESHOP'
     WHEN codification = 1 THEN 'ESHOP'
     WHEN codification = 2 THEN 'LTE'
     WHEN codification = 8 THEN 'SPLENDIST'
     WHEN codification = 13 THEN 'CALENDAR'
     ELSE 'ESHOP' END AS product_codification, 
  SUM(order_total_shipping) as value
  FROM (
 SELECT ss.dw_country_code, ss.year, ss.month, order_id, order_total_shipping,
    MAX(CASE 
           WHEN ss.product_codification = 'BYOB' THEN 1
           WHEN ss.product_codification = 'LTE' THEN 2
           WHEN ss.product_codification = 'SPLENDIST' THEN 8
           WHEN ss.product_codification = 'CALENDAR' THEN 13
           ELSE 0 END) AS codification
  FROM {{ ref('shop_sales') }} ss
  WHERE 1=1
  AND ss.order_date >= '2021-01-01'
  GROUP BY ss.dw_country_code, ss.year, ss.month, order_id, order_total_shipping
) t
  GROUP BY dw_country_code, year, month, product_codification
)



UNION ALL

-- boxes shipped
(SELECT bs.dw_country_code, bs.year, bs.month, 'METRIC' as store, LOWER(CONCAT('box-sold-', bs.dw_country_code)) as type, CASE WHEN bs.gift = 1 THEN 'GIFTCARD ACTIVATED' ELSE 'SELF' END AS product_codification, count(*) as value
FROM {{ ref('box_sales') }} bs
WHERE 1=1
 AND bs.payment_status = 'paid'
 AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, bs.gift)

UNION ALL

-- free boxes
(SELECT bs.dw_country_code, bs.year, bs.month, 'METRIC' as store, LOWER(CONCAT('box-free-', bs.dw_country_code)) as type, CASE WHEN bs.gift = 1 THEN 'GIFTCARD ACTIVATED' ELSE 'SELF' END AS product_codification, count(*) as value
FROM {{ ref('box_sales') }} bs
WHERE 1=1
 AND bs.payment_status = 'paid'
 AND bs.sub_payment_status_id = 8
 AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, bs.gift)

UNION ALL

--boxes reexp
SELECT bs.dw_country_code, bs.year, bs.month, 'METRIC' as store,  LOWER(CONCAT('box-reexp-', bs.dw_country_code)) as type, 'ALL' AS product_codification, count(distinct bs.sub_id) AS value
FROM {{ ref('tags') }} ta
JOIN {{ ref('box_sales') }} bs ON bs.sub_id = ta.link_id AND ta.type = 'SUB' AND ta.dw_country_code = bs.dw_country_code
WHERE 1=1
GROUP BY bs.dw_country_code, bs.year, bs.month

UNION ALL




-- All work orders (production of boxes, LTE, Splendist, Calendar) By country using sales for repartition
SELECT country, year, month, 'METRIC' AS store, CONCAT(LOWER(product_codification),'-produced-', LOWER(country)) AS type, product_codification, SUM(qty_repart) AS value
FROM repart_qty
GROUP BY country, year, month, product_codification


UNION ALL
-- boxes dailies


SELECT b.dw_country_code, extract(year from b.date) AS y, extract(month from b.date) as m, 'ONLINE' as store, 'NB DAILIES' AS type, 'ALL' AS product_codification, COUNT(DISTINCT ah.sub_id) AS value
FROM {{ ref('allocation_history') }} ah
JOIN {{ ref('order_detail_sub') }} s ON s.id = ah.sub_id AND s.dw_country_code = ah.dw_country_code
JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
WHERE ah.allocation_method = 'BATCH_ALLOCATION_DAILY'
AND ah.date_alloc >= '2020-12-01'
GROUP BY b.dw_country_code, y, m


UNION ALL

-- KPI box

SELECT dw_country_code, year, month, 'METRIC',
concat('acquis-new-new-',LOWER(dw_country_code)) as cat,
'BOX',
count(*)
FROM {{ ref('box_sales') }}
WHERE acquis_status_lvl1 = 'ACQUISITION'
AND acquis_status_lvl2 = 'NEW NEW'
AND diff_current_box <= 0
GROUP BY ALL

UNION ALL

SELECT dw_country_code, year, month, 'METRIC',
concat('acquis-reactivation-',LOWER(dw_country_code)) as cat,
'BOX',
count(*)
FROM {{ ref('box_sales') }}
WHERE acquis_status_lvl1 = 'ACQUISITION'
AND acquis_status_lvl2 = 'REACTIVATION'
AND diff_current_box <= 0
GROUP BY ALL


UNION ALL

SELECT dw_country_code, year, month, 'METRIC',
concat('acquis-gift-',LOWER(dw_country_code)) as cat,
'BOX',
count(*)
FROM {{ ref('box_sales') }}
WHERE acquis_status_lvl1 = 'ACQUISITION'
AND acquis_status_lvl2 = 'GIFT'
AND diff_current_box <= 0
GROUP BY ALL

UNION ALL

SELECT dw_country_code, year, month, 'METRIC',
concat('acquisition-box-',LOWER(dw_country_code)) as cat,
'BOX',
count(*)
FROM {{ ref('box_sales') }}
WHERE acquis_status_lvl1 = 'ACQUISITION'
AND diff_current_box <= 0
GROUP BY ALL

UNION ALL

SELECT dw_country_code, extract(year from next_month_date) as y, extract(month from next_month_date) as m, 'METRIC', 
concat('churn-nb-',LOWER(dw_country_code)) as cat,
'BOX',
-SUM(CASE WHEN bs.next_month_status = 'CHURN' THEN 1 ELSE 0 END) AS churn_nb
FROM {{ ref('box_sales') }} as bs
WHERE bs.diff_current_box <= 1
GROUP BY dw_country_code, next_month_date

UNION ALL

SELECT dw_country_code, extract(year from next_month_date) as y, extract(month from next_month_date) as m, 'METRIC', 
concat('churn-rate-',LOWER(dw_country_code)) as cat,
'BOX',
SUM(CASE WHEN bs.next_month_status = 'CHURN' THEN 1 ELSE 0 END)/count(*) AS churn_rate
FROM {{ ref('box_sales') }} as bs
WHERE bs.diff_current_box <= 1
GROUP BY dw_country_code, next_month_date


UNION ALL

-- boxes gross revenue
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE' as store, 'BOXES GROSS REVENUE' as type, CASE WHEN bs.gift = 1 THEN 'GIFTCARD ACTIVATED' ELSE 'SELF' END AS product_codification, SUM(bs.gross_revenue)  as value
FROM {{ ref('box_sales') }} bs
WHERE 1=1
 AND bs.payment_status = 'paid'
 AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, bs.gift

UNION ALL

-- boxes discount
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE' as store, 'BOXES DISCOUNT' as type, CASE WHEN bs.gift = 1 THEN 'GIFTCARD ACTIVATED' ELSE 'SELF' END AS product_codification, SUM(bs.discount)  as value
FROM {{ ref('box_sales') }} bs
WHERE 1=1
 AND bs.payment_status = 'paid'
 AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, bs.gift


UNION ALL

-- boxes shipping
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE' as store, 'SHIPPING BOX' as type, CASE WHEN bs.gift = 1 THEN 'GIFTCARD ACTIVATED' ELSE 'SELF' END AS product_codification, SUM(bs.shipping)  as value
FROM {{ ref('box_sales') }} bs
WHERE 1=1
 AND bs.payment_status = 'paid'
 AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, bs.gift

UNION ALL

-- product_cost online
(SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, 'PRODUCT COSTS' as type, product_codification,  SUM(ss.euro_purchase_price*ss.quantity) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
-- AND ss.year >= 2020
 AND product_codification NOT IN ('GIFT', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_status <> 'refund'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- product_cost store Paris
(SELECT ss.dw_country_code, ss.year, ss.month, 'Store Paris' AS store, 'PRODUCT COSTS' as type, product_codification,  SUM(ss.euro_purchase_price*ss.quantity) as value
FROM {{ ref('shop_sales') }} ss
WHERE 1=1
 AND product_codification NOT IN ('GIFT','LOYALTY', 'LOYALTY COUPON')
 AND ss.store_code = 'Store'
  AND ss.order_status <> 'refund'
  AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification)

UNION ALL

-- product_cost of shop reexp
SELECT  dw_country_code, year, month, NULL AS store, 'REEXP - Products cost', product_codification, SUM(products_cost) AS costs
FROM ops.shop_shipments
WHERE first_reexp = 'reexp'
AND year >= 2020
GROUP BY dw_country_code, year, month, product_codification

UNION ALL

-- shipping cost of shop reexp
SELECT  dw_country_code, year, month, NULL AS store, 'REEXP - Shipping cost', product_codification, SUM(shipping_cost) AS costs
FROM ops.shop_shipments
WHERE first_reexp = 'reexp'
AND year >= 2020
GROUP BY all

UNION ALL

-- picking cost of shop reexp
SELECT  dw_country_code, year, month, NULL AS store, 'REEXP - Picking cost', product_codification, SUM(picking_cost) AS costs
FROM ops.shop_shipments
WHERE first_reexp = 'reexp'
AND year >= 2020
GROUP BY all

UNION ALL

-- product_cost of box reexp
SELECT  dw_country_code, extract(year from shipping_date) as year, extract(month from shipping_date) as month, NULL AS store, concat(first_reexp,' - Products cost'), 'BOX', SUM(product_cost) AS costs
FROM ops.box_shipments
WHERE first_reexp IN ('reexp', 'reexp_mini')
AND year >= 2020
GROUP BY all

UNION ALL

-- shipping cost of box reexp
SELECT  dw_country_code, extract(year from shipping_date) as year, extract(month from shipping_date) as month, NULL AS store, concat(first_reexp,' - Shipping cost'), 'BOX', SUM(shipping_cost) AS costs
FROM ops.box_shipments
WHERE first_reexp IN ('reexp', 'reexp_mini')
AND year >= 2020
GROUP BY all

UNION ALL

-- picking cost of box reexp
SELECT  dw_country_code, extract(year from shipping_date) as year, extract(month from shipping_date) as month, NULL AS store, concat(first_reexp,' - Picking cost'), 'BOX', SUM(picking_cost) AS costs
FROM ops.box_shipments
WHERE first_reexp IN ('reexp', 'reexp_mini')
AND year >= 2020
GROUP BY all

UNION ALL

-- box shipped but refunded
SELECT dw_country_code, extract(year from date) as year, extract(month from date) as month,NULL, 'REFUND BOX - Products cost','BOX', SUM(euro_purchase_price) AS box_cost
FROM (
SELECT bon.dw_country_code,s.id, b.date,  MAX(iic.euro_purchase_price) AS euro_purchase_price
FROM {{ ref('b2c_order_notifications') }} bon
JOIN {{ ref('order_detail_sub') }} s ON s.id = bon.sub_id AND s.dw_country_code = bon.dw_country_code
JOIN {{ ref('products') }} p ON p.box_id = s.box_id AND p.coffret_id = s.coffret_id AND p.dw_country_code = s.dw_country_code
JOIN `teamdata-291012.catalog.inventory_item_catalog` iic ON iic.sku = p.sku 
JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN {{ ref('box_sales') }} bs ON bs.sub_id = bon.sub_id AND bon.dw_country_code = bs.dw_country_code
WHERE bon.type = 5
AND bon.sub_id > 0
AND bon.reference NOT LIKE '%REEXP%'
AND bs.sub_id IS NULL
AND s.shipping_status_id = 8
AND s.box_id >= 112
GROUP BY ALL
) t
GROUP BY ALL

UNION ALL


-- Depreciation of product costs
SELECT ss.dw_country_code, ss.year, ss.month, 'ONLINE' AS store, 'REVERSAL OF DEPRECIATION' as type, product_codification,  
SUM((COALESCE(ss.euro_purchase_price, dd.purchase_price_before)-dd.purchase_price_depreciated)*ss.quantity) as value
FROM {{ ref('shop_sales') }} ss
JOIN {{ ref('catalog') }} c ON c.product_id = ss.product_id AND c.dw_country_code = ss.dw_country_code
JOIN `ops.depreciation_detail` dd ON dd.sku = c.sku 
WHERE 1=1
AND ss.order_date >= dd.date_start
 AND product_codification NOT IN ('GIFT', 'LOYALTY COUPON')
 AND ss.store_code <> 'Store'
 AND ss.order_status <> 'refund'
 AND ss.order_date >= '2021-01-01'
GROUP BY ss.dw_country_code, type, ss.year, ss.month, product_codification

UNION ALL

SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE', 'REVERSAL OF DEPRECIATION', 'BOX', SUM((COALESCE(c.euro_purchase_price,dd.purchase_price_before)-dd.purchase_price_depreciated)*kl.quantity) as value
FROM {{ ref('kit_links') }} kl
JOIN {{ ref('products') }} p ON p.id = kl.kit_id AND p.dw_country_code = kl.dw_country_code
JOIN {{ ref('catalog') }} as c ON c.product_id = kl.product_id AND c.dw_country_code = kl.dw_country_code 
JOIN {{ ref('boxes') }} b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
JOIN {{ ref('box_sales') }} bs ON bs.box_id = p.box_id AND bs.coffret_id = p.coffret_id AND bs.dw_country_code = p.dw_country_code
JOIN `ops.depreciation_detail` dd ON dd.sku = c.sku AND b.date >= dd.date_start
WHERE 1=1
AND c.product_codification_id = 30
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month

UNION ALL

-- opex & cogs (REAL)
-- TODO : FRANCE ONLY
/*
SELECT "FR", extract(year FROM o.month) AS year,
       extract(month FROM o.month) AS m,
       'ONLINE',
       o.type || ' - ' || CASE WHEN o.category = 'Packaging' THEN o.description ELSE o.category END AS type1,
       CASE WHEN o.type = 'OPEX' OR o.product IS NULL THEN 'ALL' ELSE o.product END AS product_codification,
       SUM(o.cost) AS value
FROM bdd_prod_fr.da_cogs_total o
WHERE o.description NOT LIKE '%Empty%box%'
GROUP BY year, m, product_codification, type1


 UNION ALL
*/

-- avg shop order weight
 SELECT dw_country_code, year, month, NULL AS store, CONCAT(LOWER(product_codification),'-avg_order_weight-', LOWER(dw_country_code)), product_codification, avg(order_weight)
FROM ops.shop_shipments
GROUP BY dw_country_code, year, month, product_codification

UNION ALL

-- COGS shipping (theoretical)
SELECT dw_country_code,
       year,
       month, 
       'ONLINE',
       'COGS - SHIPPING COST',
       CASE WHEN codification = 0 THEN 'ESHOP'
            WHEN codification = 1 THEN 'BYOB'
            WHEN codification = 2 THEN 'LTE'
            WHEN codification = 8 THEN 'SPLENDIST'
            WHEN codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       SUM(shipping_cost) AS shipping_cost
FROM {{ ref('shop_orders_margin') }}
GROUP BY dw_country_code, year, month, product_codification


UNION ALL

-- COGS Picking Shop (theoretical)
SELECT 
      dw_country_code,
      year, 
       month, 
       'ONLINE',
       'COGS - PICKING THEORICAL' as type,
       CASE WHEN codification = 0 THEN 'ESHOP'
            WHEN codification = 1 THEN 'BYOB'
            WHEN codification = 2 THEN 'LTE'
            WHEN codification = 8 THEN 'SPLENDIST'
            WHEN codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       SUM(order_picking) AS picking_cost
FROM {{ ref('shop_orders_margin') }}
GROUP BY dw_country_code, year, month, product_codification

UNION ALL
-- COGS Coop - from the MM save Pipe -- without acquisition box 
SELECT dw_country_code, extract(year from date) as y, extract(month from date) as m,  NULL, 
concat('coop-save-pipe-wout_acquisbox-',LOWER(dw_country_code)) AS type, NULL, save_pipe_coop AS value
FROM allocation.campaign_recap
WHERE LOWER(project_name) NOT LIKE '%acqui%'

UNION ALL

-- COGS Coop - from the MM save Pipe --  acquisition box only
SELECT dw_country_code, extract(year from date) as y, extract(month from date) as m,  NULL, 
concat('coop-save-pipe-acquisbox-only-',LOWER(dw_country_code)) AS type, NULL, save_pipe_coop AS value
FROM allocation.campaign_recap
WHERE LOWER(project_name) LIKE '%acqui%'

UNION ALL

-- COGS Coop - from the MM save Pipe -- TOTAL with acquisition box 
SELECT dw_country_code, extract(year from date) as y, extract(month from date) as m,  NULL, 
concat('coop-save-pipe-total-',LOWER(dw_country_code)) AS type, NULL, SAFE_DIVIDE(SUM(SAFE_DIVIDE(TOTAL_Sourced_Volume,nb_products_by_box)*save_pipe_coop),SUM(SAFE_DIVIDE(TOTAL_Sourced_Volume,nb_products_by_box))) AS value
FROM allocation.campaign_recap
GROUP BY dw_country_code, y, m

UNION ALL

-- COGS Coop - products cost for the box
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE', 'COGS - Product cost', 'BOX', SUM(c.euro_purchase_price*kl.quantity) as total_purchase_prices
FROM {{ ref('kit_links') }} kl
JOIN {{ ref('products') }} p ON p.id = kl.kit_id AND p.dw_country_code = kl.dw_country_code
JOIN {{ ref('catalog') }} as c ON c.product_id = kl.product_id AND c.dw_country_code = kl.dw_country_code 
JOIN {{ ref('boxes') }} b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
JOIN {{ ref('box_sales') }} bs ON bs.box_id = p.box_id AND bs.coffret_id = p.coffret_id AND bs.dw_country_code = p.dw_country_code
WHERE 1=1
AND c.product_codification_id = 30
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month


UNION ALL

-- COGS  - TOTAL products cost for the box - REEXP - coop + assembly + shipper + box included
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE', 'COGS - TOTAL Product cost', 'BOX REEXP', SUM(COALESCE(c.euro_purchase_price,0)) as total_purchase_prices
FROM {{ ref('box_sales') }} bs 
JOIN {{ ref('tags') }} t ON t.link_id = bs.sub_id AND t.dw_country_code = bs.dw_country_code AND t.type = 'SUB' AND t.value = 'reexp'
JOIN {{ ref('products') }} p ON p.box_id = bs.box_id AND p.coffret_id = bs.coffret_id AND p.dw_country_code = bs.dw_country_code 
JOIN {{ ref('catalog') }} c ON c.product_id = p.id AND c.dw_country_code = p.dw_country_code
WHERE 1=1
AND c.sku NOT LIKE '%MENU%'
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month

UNION ALL

-- COGS Shipper - shipper cost for the box
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE', 'COGS - Shipper estimated', 'BOX', SUM(c.euro_purchase_price*kl.quantity) as total_purchase_prices
FROM {{ ref('kit_links') }} kl
JOIN {{ ref('products') }} p ON p.id = kl.kit_id AND p.dw_country_code = kl.dw_country_code
JOIN {{ ref('catalog') }} as c ON c.product_id = kl.product_id AND c.dw_country_code = kl.dw_country_code 
JOIN {{ ref('boxes') }} b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
JOIN {{ ref('box_sales') }} bs ON bs.box_id = p.box_id AND bs.coffret_id = p.coffret_id AND bs.dw_country_code = p.dw_country_code
WHERE 1=1
AND c.product_codification_id = 33
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month

UNION ALL

-- COGS Empty Box -  Empty Box cost for the box
SELECT bs.dw_country_code, bs.year, bs.month, 'ONLINE', 'COGS - Empty boxes', 'BOX', SUM(c.euro_purchase_price*kl.quantity) as total_purchase_prices
FROM {{ ref('kit_links') }} kl
JOIN {{ ref('products') }} p ON p.id = kl.kit_id AND p.dw_country_code = kl.dw_country_code
JOIN {{ ref('catalog') }} as c ON c.product_id = kl.product_id AND c.dw_country_code = kl.dw_country_code 
JOIN {{ ref('boxes') }} b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
JOIN {{ ref('box_sales') }} bs ON bs.box_id = p.box_id AND bs.coffret_id = p.coffret_id AND bs.dw_country_code = p.dw_country_code
WHERE 1=1
AND c.product_codification_id = 31
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month


UNION ALL

-- picking box theorical
SELECT bs.dw_country_code,extract(year from bs.box_date), extract(month from bs.box_date), NULL, 'COGS - Picking BOX', bs.mono_multi, 
SUM(bs.picking_cost) AS picking_cost

FROM ops.box_shipments bs

GROUP BY bs.dw_country_code, bs.box_date, bs.mono_multi

UNION ALL

-- shipping box theorical
SELECT bs.dw_country_code,extract(year from bs.box_date), extract(month from bs.box_date), NULL, 'COGS - shipping BOX', bs.first_reexp AS type,
SUM(bs.shipping_cost) AS shipping_cost

FROM ops.box_shipments bs

GROUP BY bs.dw_country_code, bs.box_date, bs.first_reexp

UNION ALL
---- unit COGS BOX -----------

SELECT bs.dw_country_code, bs.year, bs.month, bs.dw_country_code, 
concat('unit-cogs-box-',CASE WHEN iic.logistic_category = 'product' THEN 'coop' WHEN logistic_category = 'consumable item' THEN 'shipper' ELSE iic.logistic_category END,'-',LOWER(bs.dw_country_code)), 
CONCAT('box-', CASE WHEN iic.logistic_category = 'product' THEN 'coop' WHEN logistic_category = 'consumable item' THEN 'shipper' ELSE iic.logistic_category END), 
SAFE_DIVIDE(SUM(iic.euro_purchase_price), COUNT(DISTINCT bs.sub_id)) as value
FROM {{ ref('kit_details') }} kd
JOIN `teamdata-291012.catalog.inventory_item_catalog` iic ON iic.sku = kd.component_sku
JOIN {{ ref('box_sales') }} bs ON bs.box_id = kd.box_id AND bs.coffret_id = kd.coffret_id AND bs.dw_country_code = kd.dw_country_code
WHERE 1=1
AND bs.box_id >= 112
GROUP BY bs.dw_country_code, bs.year, bs.month, iic.logistic_category



UNION ALL

-- all exceptions (the source is in the P&L Live document)

SELECT e.country_code, e.year, e.month, e.store, e.type, e.product_codification, e.value
FROM `pnl.exceptions` e

UNION ALL
/*
-- Box shipping details on a specific month -- TO DELETE WHEN NEXT QUERY IS OK
SELECT s.dw_country_code,  
extract(year from bon.event_date) as y,
extract(month from bon.event_date) as m,
CASE WHEN bon.reference LIKE '%REEXP-SUB%' THEN 'REEXP' ELSE 'SUB' END AS cat,
CASE WHEN bon.reference LIKE '%REEXP-SUB%' THEN 'REEXP' 
ELSE
    CASE WHEN FORMAT_DATE('%Y-%m-%d',b.date) < FORMAT_DATE('%Y-%m-01',bon.event_date) THEN 'shipping M-1'
         WHEN FORMAT_DATE('%Y-%m-%d',b.date) = FORMAT_DATE('%Y-%m-01',bon.event_date) THEN 'shipping M'
         WHEN FORMAT_DATE('%Y-%m-%d',b.date) > FORMAT_DATE('%Y-%m-01',bon.event_date) THEN 'shipping M+1'
    ELSE 'OTHER' 
    END
END AS cat2,
'BOX' AS cat3,
count(*) AS value
FROM {{ ref('b2c_order_notifications') }} bon
JOIN {{ ref('order_detail_sub') }} s ON s.id = bon.sub_id AND s.dw_country_code = bon.dw_country_code
JOIN {{ ref('boxes') }} b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
WHERE bon.type = 5
AND (bon.reference LIKE '%SUB-%' OR bon.reference LIKE '%REEXP-SUB-%')
GROUP BY s.dw_country_code, y, m, cat, cat2, cat3

UNION ALL*/

-- theorical box shipping details on a specific month
SELECT bs.dw_country_code, bs.year, bs.month, NULL,first_reexp,
concat('box-shipping-', lower(bs.dw_country_code),'-', 
CASE 
  WHEN bs.first_reexp = 'reexp' THEN 'reexp' 
  WHEN bs.box_date < DATE_TRUNC(bs.shipping_date,month) THEN 'm-1'
  WHEN bs.box_date = DATE_TRUNC(bs.shipping_date,month) THEN 'm'
  WHEN bs.box_date > DATE_TRUNC(bs.shipping_date,month) THEN 'm+1'
END) as codif,
SUM(bs.shipping_cost)
FROM `teamdata-291012.ops.box_shipments` bs
GROUP BY bs.dw_country_code, bs.year, bs.month, codif, first_reexp

UNION ALL

-- real invoices from invoice manager

SELECT * 
FROM pnl.invoice_manager

UNION ALL

-- Marketing OPEX - From Grand Livre
SELECT LEFT(analytic,2) AS country, extract(year from period) as y, extract(month from period) as m, NULL, analytic, 'GL', SUM(total) AS value
FROM `pnl.gl_compact`
WHERE -- analytic NOT LIKE 'FR65%'
-- AND analytic NOT LIKE 'DE65%'
-- AND 
company = 'BC'
-- WHERE centre_analytic IN ('FR61100','FR61110','FR61120','FR61121','FR61130','FR61131','FR61140','FR61160','FR61170','DE61100','DE61110','DE61120','DE61130','DE61140','DE61150','DE61170','DE61180','ES61100','IT61100')
GROUP BY analytic, y, m

UNION ALL


/*
select country,extract(year from d) year,extract(month from d)month ,'ONLINE' store,'CAC_LIVE'center,'BOX',round(sum(spent)/sum(total_this_year) ,1) as cac_LIVE
from `marketing.Marketing_cac_live` 
group by country,year,month,store,center


*/

-- gift cards not used/expired
  
 SELECT gc.dw_country_code, extract(year from gc.expiration_Date) AS y,extract(month from  gc.expiration_Date) AS m,
'ONLINE', 'GIFT_CARDS_EXPIRED',
  'BOX', 
SUM(gc.amount/(1+COALESCE(vat.taux,20)/100)) AS amount_ht
FROM {{ ref('gift_cards') }} gc
JOIN {{ ref('order_details') }} d ON d.id = gc.order_detail_id AND d.dw_country_code = gc.dw_country_code
JOIN {{ ref('orders') }} o ON o.id = d.order_id AND o.dw_country_code = d.dw_country_code
LEFT JOIN {{ ref('box_sales') }} as bs ON bs.dw_country_code = gc.dw_country_code AND bs.gift_card_id = gc.id
LEFT JOIN bdd_prod_fr.wp_jb_tva_product vat ON vat.country_code = o.shipping_country  AND vat.category = 'normal'
WHERE o.status_id IN (1,3)
AND bs.user_id IS NULL
AND DATE(gc.expiration_date) < current_date
GROUP BY 1,2,3,4,5


UNION ALL

(
SELECT 
"0", 0, 0, "0", "CURRENT_TIMESTAMP", 
CAST(FORMAT_TIMESTAMP('%F %T %Ez', CURRENT_TIMESTAMP(), 'Europe/Paris') as STRING), 
0
)

UNION ALL

-- New marketing expenses with Funnel data
SELECT CASE 
  WHEN Market = 'Germany' THEN 'DE' 
  WHEN Market = 'Spain' THEN 'ES'
  WHEN Market = 'Italy' THEN 'IT'
  ELSE 'FR' END AS country,
extract(year from b.date) as y, 
Extract(month from b.date) AS m,
UPPER(Campaign_type_) as store,
UPPER(Channel) as type,
UPPER(Product_type) as product_codification,
SUM(cost) AS value
FROM `teamdata-291012.funnel.funnel_data` d
inner join inter.boxes b on d.date>=b.shipping_date AND  d.date <= b.closing_date
AND case when market='France' then 'FR' when market='Spain' then 'ES' when market ='Germany' then 'DE'end=b.dw_country_code
WHERE b.date >= '2023-01-01'
GROUP BY country, y, m, store, type, product_codification



UNION ALL

SELECT 'FR', extract(year from month) AS y, extract(month from month) AS m, NULL AS store, 'ASILAGE', 'REVENUE', SUM(COALESCE(revenue,0))
FROM teamdata-291012.Spreadsheet_synchro.asilage
WHERE revenue IS NOT NULL
GROUP BY ALL

UNION ALL

SELECT 'FR', extract(year from month) AS y, extract(month from month) AS m, NULL AS store, 'ASILAGE', 'COGS', SUM(COALESCE(cogs,0))
FROM teamdata-291012.Spreadsheet_synchro.asilage
WHERE revenue IS NOT NULL
GROUP BY ALL
