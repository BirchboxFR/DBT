{{ config(
    materialized='table',
    partition_by={
        "field": "shipping_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["dw_country_code", "shipping_mode_nice_name"],
    persist_docs={"relation": true, "columns": true},
    on_schema_change='ignore'
) }}

WITH 
first_shipping_mode AS
(
  SELECT bec.dw_country_code, bec.sub_id, min(sm.id) AS id
  FROM inter.b2c_exported_orders bec 
  LEFT JOIN inter.shipping_modes sm ON sm.b2c_method = bec.carrier_code AND sm.dw_country_code = bec.dw_country_code AND sm.id NOT IN (44,66,54) AND bec.recipient_address_country = sm.country
  WHERE bec.reference NOT LIKE '%REEXP%'
  -- AND sub_id = 1541
  -- AND bec.dw_country_code = 'PL'
  GROUP BY ALL
),
pot_box_shipments AS 
(
  SELECT bon.dw_country_code,bon.reference, bon.sub_id, DATE(min(bon.event_date)) AS shipping_date
  FROM `inter.b2c_order_notifications` bon
  WHERE bon.type = 5
  
  AND bon.sub_id > 0
  AND bon.reference NOT LIKE '%REEXP%'
  GROUP BY bon.dw_country_code,bon.reference, bon.sub_id
),
pot_box_reexp_shipments AS 
(
  SELECT bon.dw_country_code, bon.sub_id, DATE(min(bon.event_date)) AS shipping_date
  FROM `inter.b2c_order_notifications` bon
  WHERE bon.type = 5
  
  AND bon.sub_id > 0
  AND bon.reference LIKE '%REEXP%'
  GROUP BY bon.dw_country_code, bon.sub_id
),
pot_shop_reexp_shipments AS 
(
  SELECT bon.dw_country_code,bon.reference, bon.order_id, bon.order_detail_id, DATE(min(bon.event_date)) AS shipping_date
  FROM `inter.b2c_order_notifications` bon
  WHERE bon.type = 5
  AND bon.sub_id IS NULL
  AND bon.reference  LIKE '%REEXP%'
  GROUP BY bon.dw_country_code,bon.reference, bon.order_id, bon.order_detail_id
),
gws_costs_table AS (
  SELECT sol.dw_country_code,
         sol.sub_id,
         SUM(d.quantity) AS nb_gws,
         COALESCE(SUM(c.purchase_price * d.quantity), 0) AS gws_costs,
         SUM(d.quantity*c.weight) AS gws_weight
  FROM inter.sub_order_link sol
  INNER JOIN inter.orders o ON sol.dw_country_code = o.dw_country_code AND sol.order_id = o.id
  INNER JOIN inter.order_details d ON o.dw_country_code = d.dw_country_code AND o.id = d.order_id
  INNER JOIN product.catalog c ON d.dw_country_code = c.dw_country_code AND d.product_id = c.product_id
  WHERE d.special_type = 'GWS'
  GROUP BY sol.dw_country_code,
           sol.sub_id
),
box_weight AS 
(
  SELECT p.dw_country_code, p.box_id, p.coffret_id, COALESCE(max(CASE WHEN p.weight = 0 THEN NULL ELSE p.weight END ),CASE WHEN b.box_quantity = 1 THEN 382 WHEN b.box_quantity = 2 THEN 800 END) AS weight
  FROM inter.products p
  JOIN inter.boxes b ON b.id = p.box_id AND b.dw_country_code = p.dw_country_code
  WHERE p.product_codification_id = 29
  AND p.box_id > 0
  AND p.coffret_id > 0
  GROUP BY p.dw_country_code, p.box_id, p.coffret_id, b.box_quantity
  ORDER BY p.dw_country_code, p.box_id, p.coffret_id
),
box_cat AS
(
SELECT
  dw_country_code,
  sub_id,
  date_alloc,
  CASE WHEN allocation_method = 'BATCH_ALLOCATION_MONTHLY' THEN 'monthly' ELSE 'daily' END as alloc_cat
  FROM
(
  SELECT
    dw_country_code,
    sub_id,
    date_alloc,
    allocation_method,
    ROW_NUMBER() OVER (PARTITION BY dw_country_code, sub_id ORDER BY date_alloc) AS row_num
  FROM
    inter.allocation_history
)
WHERE row_num = 1

),
box_costs AS (
  SELECT p.dw_country_code, p.box_id, p.coffret_id, p.sku, max(iic.euro_purchase_price) AS euro_purchase_price
FROM inter.products p
JOIN `teamdata-291012.catalog.inventory_item_catalog` iic ON iic.sku = p.sku
WHERE p.box_id > 0 AND p.coffret_id > 0
GROUP BY all
)



SELECT orders.*, 
CASE WHEN orders.nb_dailies > 0 THEN 'daily' ELSE 'monthly' END as daily_monthly,
COALESCE(CASE WHEN orders.nb_dailies > 0 THEN COALESCE(sc.price_daily, COALESCE(sc.price,0)) ELSE sc.price END,2.65) AS shipping_transport_cost, 
COALESCE(sc.shipping_taxes_rate * CASE WHEN orders.nb_dailies > 0 THEN COALESCE(sc.price_daily, sc.price) ELSE sc.price END,0) AS total_shipping_taxes,
(1 + COALESCE(sc.shipping_taxes_rate,0)) * CASE WHEN orders.nb_dailies > 0 THEN COALESCE(sc.price_daily, COALESCE(sc.price,0)) ELSE COALESCE(sc.price,0) END AS shipping_cost,
'first' as first_reexp,
CASE WHEN quantity = 1 THEN 'mono' ELSE 'multi' END AS mono_multi, 
COALESCE(smn.nice_name, orders.shipping_mode_name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight

FROM
(
SELECT bs.dw_country_code,
bs.order_id,
bs.order_detail_id,
bs.sub_id,
bs.date as box_date,
COALESCE(pbs.shipping_date, bs.date) AS shipping_date,
extract(year from COALESCE(pbs.shipping_date, bs.date)) AS year,
extract(month from COALESCE(pbs.shipping_date, bs.date)) AS month,
bs.gross_revenue,
bs.net_revenue,
bs.shipping AS shipping_revenue,
sm.id as shipping_mode_id,
sm.title AS shipping_mode_name,
COALESCE(sm.b2c_method,code) AS shipping_mode_code,
COALESCE(gct.nb_gws,0) + 1 AS quantity,
COALESCE(gct.gws_weight,0)/1000 AS gws_weight, -- kg
COALESCE(box_weight.weight, 382) /1000 AS box_weight, -- kg
COALESCE(gct.gws_weight,0)/1000 + COALESCE(box_weight.weight, 382) /1000 AS order_weight, -- kg
CASE WHEN box_cat.alloc_cat = 'monthly' THEN 0 ELSE 1 END AS nb_dailies,
CASE WHEN box_cat.alloc_cat = 'monthly' THEN 0 -- picking_daily_multi.price 
WHEN COALESCE(gct.nb_gws,0) = 0 THEN picking_daily_mono.price ELSE picking_daily_multi.price + COALESCE(gct.nb_gws,0)*picking_daily_art_supp.price END AS picking_cost,
bs.shipping_country,
COALESCE(gct.gws_costs,0) + COALESCE(box_costs.euro_purchase_price,0) AS product_cost

FROM sales.box_sales as bs
LEFT JOIN first_shipping_mode sm1 ON sm1.sub_id = bs.sub_id AND sm1.dw_country_code = bs.dw_country_code
LEFT JOIN pot_box_shipments pbs ON pbs.sub_id = bs.sub_id AND pbs.dw_country_code = bs.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.id = COALESCE(sm1.id,bs.shipping_mode) AND sm.dw_country_code = bs.dw_country_code
LEFT JOIN gws_costs_table gct ON gct.sub_id = bs.sub_id AND gct.dw_country_code = bs.dw_country_code
LEFT JOIN box_weight ON box_weight.dw_country_code = bs.dw_country_code AND box_weight.box_id = bs.box_id AND box_weight.coffret_id = bs.coffret_id
LEFT JOIN ops.logistics_costs picking_daily_mono ON picking_daily_mono.name = 'picking_daily_mono' AND bs.date >= DATE(picking_daily_mono.date_start) AND (bs.date <= DATE(picking_daily_mono.date_end) OR picking_daily_mono.date_end IS NULL)
LEFT JOIN ops.logistics_costs picking_daily_multi ON picking_daily_multi.name = 'picking_daily_multi' AND bs.date >= DATE(picking_daily_multi.date_start) AND (bs.date <= DATE(picking_daily_multi.date_end) OR picking_daily_multi.date_end IS NULL)
LEFT JOIN ops.logistics_costs picking_daily_art_supp ON picking_daily_art_supp.name = 'picking_daily_art_supp' AND bs.date >= DATE(picking_daily_art_supp.date_start) AND (bs.date <= DATE(picking_daily_art_supp.date_end) OR picking_daily_art_supp.date_end IS NULL)
LEFT JOIN box_cat ON box_cat.dw_country_code = bs.dw_country_code AND box_cat.sub_id = bs.sub_id
LEFT JOIN box_costs ON box_costs.dw_country_code = bs.dw_country_code AND box_costs.box_id = bs.box_id AND box_costs.coffret_id = bs.coffret_id

WHERE bs.payment_status = 'paid'

)
AS orders
LEFT JOIN ops.shipping_costs sc ON orders.shipping_date >= sc.date_start AND (sc.date_end IS NULL OR orders.shipping_date <= sc.date_end) AND orders.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR orders.order_weight < sc.max_weight) AND sc.shipping_mode_id = orders.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = orders.shipping_mode_id


UNION ALL

SELECT orders.*, 
'daily'  as daily_monthly,
sc.price AS shipping_transport_cost,
sc.shipping_taxes_rate * sc.price AS total_shipping_taxes,
(1 + sc.shipping_taxes_rate) * sc.price AS shipping_cost,
 'reexp' AS first_reexp,
CASE WHEN quantity = 1 THEN 'mono' ELSE 'multi' END AS mono_multi, 
COALESCE(smn.nice_name, orders.shipping_mode_name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight
FROM
(

SELECT t.dw_country_code,
NULL AS order_id,
NULL as detail_id,
s.id AS sub_id,
b.date as box_date,
SAFE_CAST(COALESCE(pot_box_reexp_shipments.shipping_date,DATE(t.timestamp)) AS DATE) AS shipping_date,
extract(year from COALESCE(pot_box_reexp_shipments.shipping_date,DATE(t.timestamp))) AS year,
extract(month from COALESCE(pot_box_reexp_shipments.shipping_date,DATE(t.timestamp))) AS month,
0 AS gross_revenue,
0 AS net_revenue,
0 AS shipping_revenue,
sm.id as shipping_mode_id,
sm.title AS shipping_mode_name,
COALESCE(sm.b2c_method,sm.code) AS shipping_mode_code,
COALESCE(gct.nb_gws,0) + 1 AS quantity,
COALESCE(gct.gws_weight,0)/1000 AS gws_weight, -- kg
COALESCE(box_weight.weight, 382) /1000 AS box_weight, -- kg
COALESCE(gct.gws_weight,0)/1000 + COALESCE(box_weight.weight, 382) /1000 AS order_weight, -- kg
CASE WHEN box_cat.alloc_cat = 'monthly' THEN 0 ELSE 1 END AS nb_dailies,
CASE -- WHEN box_cat.alloc_cat = 'monthly' THEN 0 
WHEN COALESCE(gct.nb_gws,0) = 0 THEN picking_daily_mono.price ELSE picking_daily_multi.price + COALESCE(gct.nb_gws,0)*picking_daily_art_supp.price END AS picking_cost,
s.shipping_country,
COALESCE(gct.gws_costs,0) + COALESCE(box_costs.euro_purchase_price,0) AS product_cost

FROM inter.tags t
JOIN inter.order_detail_sub s ON s.id = t.link_id AND s.dw_country_code = t.dw_country_code
JOIN `inter.shipping_modes` sm_original ON sm_original.id = s.shipping_mode AND sm_original.dw_country_code = s.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.dw_country_code = s.dw_country_code 
AND 
  (
    (sm_original.tracked  AND sm.id = sm_original.id)
  OR (sm_original.tracked is false AND sm_original.linked_shipping_mode_id IS NOT NULL AND sm.id = sm_original.linked_shipping_mode_id)
  OR (sm_original.tracked is false AND sm_original.linked_shipping_mode_id IS NULL AND sm.id = 1)
  )
JOIN inter.boxes b ON b.id = s.box_id AND b.dw_country_code = s.dw_country_code
LEFT JOIN pot_box_reexp_shipments ON pot_box_reexp_shipments.sub_id = s.id AND pot_box_reexp_shipments.dw_country_code = s.dw_country_code
LEFT JOIN box_weight ON box_weight.dw_country_code = s.dw_country_code AND box_weight.box_id = s.box_id AND box_weight.coffret_id = s.coffret_id
LEFT JOIN gws_costs_table gct ON gct.sub_id = s.id AND gct.dw_country_code = s.dw_country_code
LEFT JOIN ops.logistics_costs picking_daily_mono ON picking_daily_mono.name = 'picking_daily_mono' AND b.date >= DATE(picking_daily_mono.date_start) AND (b.date <= DATE(picking_daily_mono.date_end) OR picking_daily_mono.date_end IS NULL)
LEFT JOIN ops.logistics_costs picking_daily_multi ON picking_daily_multi.name = 'picking_daily_multi' AND b.date >= DATE(picking_daily_multi.date_start) AND (b.date <= DATE(picking_daily_multi.date_end) OR picking_daily_multi.date_end IS NULL)
LEFT JOIN ops.logistics_costs picking_daily_art_supp ON picking_daily_art_supp.name = 'picking_daily_art_supp' AND b.date >= DATE(picking_daily_art_supp.date_start) AND (b.date <= DATE(picking_daily_art_supp.date_end) OR picking_daily_art_supp.date_end IS NULL)
LEFT JOIN box_cat ON box_cat.dw_country_code = s.dw_country_code AND box_cat.sub_id = s.id
LEFT JOIN box_costs ON box_costs.dw_country_code = t.dw_country_code AND box_costs.box_id = s.box_id AND box_costs.coffret_id = s.coffret_id

WHERE t.type = 'SUB'
AND t.value = 'reexp'

)
AS orders
LEFT JOIN ops.shipping_costs sc ON orders.shipping_date >= sc.date_start AND (sc.date_end IS NULL OR orders.shipping_date <= sc.date_end) AND orders.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR orders.order_weight < sc.max_weight) AND sc.shipping_mode_id = orders.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = orders.shipping_mode_id
WHERE orders.shipping_date <= CURRENT_DATE

UNION ALL

-- reexp mini

SELECT orders.*, 
'reexp_mini'  as daily_monthly,
sc.price AS shipping_transport_cost,
sc.shipping_taxes_rate * sc.price AS total_shipping_taxes,
(1 + sc.shipping_taxes_rate) * sc.price AS shipping_cost,
 'reexp_mini' AS first_reexp,
CASE WHEN quantity = 1 THEN 'mono' ELSE 'multi' END AS mono_multi, 
COALESCE(smn.nice_name, orders.shipping_mode_name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight
FROM
(
SELECT mr.dw_country_code,
NULL AS order_id,
NULL AS detail_id,
bs.sub_id,
bs.date AS box_date,
SAFE_CAST(COALESCE(mr.reexp_date, mr.date) AS DATE) AS shipping_date,
extract(year from COALESCE(mr.reexp_date, mr.date)) AS year,
extract(month from COALESCE(mr.reexp_date, mr.date)) AS month,
0 AS gross_revenue,
0 AS net_revenue,
0 AS shipping_revenue,
sm.id as shipping_mode_id,
sm.title AS shipping_mode_name,
COALESCE(sm.b2c_method,sm.code) AS shipping_mode_code,
1 AS quantity,
0 AS gws_weight,
0 AS box_weight,
COALESCE(iic.weight/1000,0.2) AS order_weight, -- avg weight 200g
0 AS nb_dailies,
0 AS picking_cost, -- to be done
bs.shipping_country,
iic.euro_purchase_price AS product_cost

FROM inter.mini_reexp mr
JOIN inter.boxes b ON b.id = mr.box_id AND mr.dw_country_code = b.dw_country_code
JOIN inter.products p ON p.id = mr.product_id AND p.dw_country_code = mr.dw_country_code
JOIN sales.box_sales bs ON bs.sub_id = mr.sub_id AND bs.dw_country_code = mr.dw_country_code
JOIN `teamdata-291012.catalog.inventory_item_catalog` iic ON iic.sku = p.sku
JOIN `inter.shipping_modes` sm_original ON sm_original.id = bs.shipping_mode AND sm_original.dw_country_code = bs.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.dw_country_code = bs.dw_country_code 
AND 
  (
    (COALESCE(mr.reexp_date, mr.date) < '2024-01-08' AND sm_original.tracked  AND sm.id = sm_original.id)
  OR (sm_original.tracked is false AND COALESCE(mr.reexp_date, mr.date) < '2024-01-08' AND sm_original.linked_shipping_mode_id IS NOT NULL AND sm.id = sm_original.linked_shipping_mode_id)
  OR (sm_original.tracked is false AND COALESCE(mr.reexp_date, mr.date) < '2024-01-08' AND sm_original.linked_shipping_mode_id IS NULL AND sm.id = 1)
  OR (COALESCE(mr.reexp_date, mr.date) >= '2024-03-01' AND  sm.id = 2)
  )

WHERE /*mr.dw_country_code = 'FR'
AND */
mr.status_id = 1
) orders
LEFT JOIN ops.shipping_costs sc ON DATE(orders.shipping_date) >= sc.date_start AND (sc.date_end IS NULL OR DATE(orders.shipping_date) <= sc.date_end) AND orders.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR orders.order_weight < sc.max_weight) AND sc.shipping_mode_id = orders.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = orders.shipping_mode_id
WHERE DATE(orders.shipping_date) <= CURRENT_DATE
