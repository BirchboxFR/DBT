WITH 
first_shipping_mode AS
(
  SELECT bec.dw_country_code, 
  CAST(REGEXP_EXTRACT(reference, r'ORD-(\d+)') AS INT64) AS order_id, 
  min(sm.id) AS id
  FROM inter.b2c_exported_orders bec 
  LEFT JOIN inter.shipping_modes sm ON sm.b2c_method = bec.carrier_code AND sm.dw_country_code = bec.dw_country_code
  WHERE bec.reference NOT LIKE '%REEXP%'
  AND bec.reference LIKE '%ORD%'
  GROUP BY ALL
),
pot_shop_shipments AS 
(
  SELECT bon.dw_country_code,bon.reference, bon.order_id, DATE(min(bon.event_date)) AS shipping_date
  FROM `inter.b2c_order_notifications` bon
  WHERE bon.type = 5
  AND bon.order_detail_id IS NULL
  AND bon.sub_id IS NULL
  AND bon.reference NOT LIKE '%REEXP%'
  GROUP BY bon.dw_country_code,bon.reference, bon.order_id
),
pot_shop_reexp_shipments AS 
(
  SELECT bon.dw_country_code,bon.reference, bon.order_id, bon.order_detail_id, DATE(min(bon.event_date)) AS shipping_date
  FROM `inter.b2c_order_notifications` bon
  WHERE bon.type = 5
  AND bon.sub_id IS NULL
  AND bon.reference  LIKE '%REEXP%'
  GROUP BY bon.dw_country_code,bon.reference, bon.order_id, bon.order_detail_id
)


-- shop first shipments
SELECT som.dw_country_code, 
som.order_id, 
NULL as order_detail_id,
-- NULL AS sub_id,
som.order_date,
COALESCE(psc.shipping_date, som.order_date) as shipping_date,
extract(year from COALESCE(psc.shipping_date, som.order_date)) AS year,
extract(month from COALESCE(psc.shipping_date, som.order_date)) AS month,
som.gross_revenue,
som.net_revenue,
som.shipping_revenue,
sm.id as shipping_mode_id,
COALESCE(sm.b2c_method,code) AS shipping_mode_code,
sm.title as shipping_mode_name,
-- NULL AS quantity,
som.order_weight,
som.order_picking AS picking_cost,
sm.country AS shipping_country,
som.packaging_cost,
som.shipping_taxes_rate,
som.shipping_transport_cost,
som.total_shipping_taxes,
som.shipping_cost,
'first' AS first_reexp,
COALESCE(smn.nice_name, sm.name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight,
CASE WHEN codification = 0 THEN 'ESHOP'
            WHEN codification = 1 THEN 'BYOB'
            WHEN codification = 2 THEN 'LTE'
            WHEN codification = 8 THEN 'SPLENDIST'
            WHEN codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       som.products_cost,
som.quantity,
CASE WHEN COALESCE(som.quantity,0) + COALESCE(som.msg_perso,0) > 1 THEN 'multi' ELSE 'mono' END AS mono_multi,
som.msg_perso

FROM `sales.shop_orders_margin` som
LEFT JOIN first_shipping_mode sm1 ON sm1.order_id = som.order_id AND sm1.dw_country_code = som.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.id = COALESCE(sm1.id,som.shipping_mode_id) AND sm.dw_country_code = som.dw_country_code
LEFT JOIN pot_shop_shipments psc ON psc.order_id = som.order_id AND psc.dw_country_code = som.dw_country_code
LEFT JOIN ops.shipping_costs sc ON COALESCE(psc.shipping_date, som.order_date) >= sc.date_start AND (sc.date_end IS NULL OR COALESCE(psc.shipping_date, som.order_date) <= sc.date_end) AND som.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR som.order_weight < sc.max_weight) AND sc.shipping_mode_id = som.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = som.shipping_mode_id

UNION ALL


-- reexp shop
SELECT som.dw_country_code, 
som.order_id, 
NULL as order_detail_id,
-- NULL AS sub_id,
DATE(t.timestamp) AS order_date,
COALESCE(psc.shipping_date, DATE(t.timestamp)) as shipping_date,
extract(year from COALESCE(psc.shipping_date, DATE(t.timestamp))) AS year,
extract(month from COALESCE(psc.shipping_date, DATE(t.timestamp))) AS month,
som.gross_revenue,
som.net_revenue,
som.shipping_revenue,
sm.id as shipping_mode_id,
COALESCE(sm.b2c_method,code) AS shipping_mode_code,
sm.title as shipping_mode_name,
-- NULL AS quantity,
som.order_weight,
som.order_picking AS picking_cost,
sm.country AS shipping_country,
som.packaging_cost,
som.shipping_taxes_rate,
som.shipping_transport_cost,
som.total_shipping_taxes,
som.shipping_cost,
'reexp' AS first_reexp,
COALESCE(smn.nice_name, sm.name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight,
CASE WHEN codification = 0 THEN 'ESHOP'
            WHEN codification = 1 THEN 'BYOB'
            WHEN codification = 2 THEN 'LTE'
            WHEN codification = 8 THEN 'SPLENDIST'
            WHEN codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       som.products_cost,
    som.quantity,
CASE WHEN COALESCE(som.quantity,0) + COALESCE(som.msg_perso,0) > 1 THEN 'multi' ELSE 'mono' END AS mono_multi,
som.msg_perso
FROM inter.tags t
JOIN `sales.shop_orders_margin` som ON som.order_id = t.link_id AND som.dw_country_code = t.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.id = som.shipping_mode_id AND sm.dw_country_code = som.dw_country_code
LEFT JOIN pot_shop_reexp_shipments psc ON psc.order_id = som.order_id AND psc.dw_country_code = som.dw_country_code AND psc.order_detail_id IS NULL
LEFT JOIN ops.shipping_costs sc ON COALESCE(psc.shipping_date, DATE(t.timestamp)) >= sc.date_start AND (sc.date_end IS NULL OR COALESCE(psc.shipping_date, DATE(t.timestamp)) <= sc.date_end) AND som.order_weight >= sc.min_weight AND (sc.max_weight IS NULL OR som.order_weight < sc.max_weight) AND sc.shipping_mode_id = som.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = som.shipping_mode_id
WHERE t.type = 'ORD'
AND t.value = 'reexp'


UNION ALL

-- reexp shop - DET - REEXP-DET
SELECT som.dw_country_code, 
som.order_id, 
NULL as order_detail_id,
-- NULL AS sub_id,
DATE(t.timestamp) AS order_date,
COALESCE(psc.shipping_date, DATE(t.timestamp)) as shipping_date,
som.year,
som.month,
som.gross_revenue,
som.net_revenue,
som.shipping_revenue,
sm.id as shipping_mode_id,
COALESCE(sm.b2c_method,code) AS shipping_mode_code,
sm.title as shipping_mode_name,
-- NULL AS quantity,
(d.quantity * COALESCE(c.weight, 0.2) + 0.15)/1000 AS order_weight,
dlc_first_product.price 
+
(dlc_prepacked.price - dlc_first_product.price) * 
(CASE WHEN d.quantity = 1 THEN 1 ELSE 0 END )
*
CASE WHEN pc.PC_SPLENDIST OR pc.PC_CALENDAR THEN 1 ELSE 0 END -- deduction if only one calendar or splendist
+
(d.quantity-1) * dlc_next_product.price  AS picking_cost,
sm.country AS shipping_country,
som.packaging_cost,
sc.shipping_taxes_rate AS shipping_taxes_rate,
sc.price AS shipping_transport_cost,
sc.shipping_taxes_rate * sc.price AS total_shipping_taxes,
(1 + sc.shipping_taxes_rate) * sc.price  AS shipping_cost,
'reexp' AS first_reexp,
COALESCE(smn.nice_name, sm.name) AS shipping_mode_nice_name,
sc.min_weight AS min_weight_range,
sc.max_weight AS max_weight_range,
concat('From ',LPAD(CAST(ROUND(sc.min_weight * 100) AS STRING), 4, '0'),' g',
CASE WHEN max_weight IS NOT NULL THEN
concat(' to ',LPAD(CAST(ROUND(sc.max_weight * 100) AS STRING), 4, '0'),' g')
ELSE '' END
) AS range_of_weight,
CASE WHEN som.codification = 0 THEN 'ESHOP'
            WHEN som.codification = 1 THEN 'BYOB'
            WHEN som.codification = 2 THEN 'LTE'
            WHEN som.codification = 8 THEN 'SPLENDIST'
            WHEN som.codification = 13 THEN 'CALENDAR'
            ELSE 'ESHOP'
       END AS product_codification,
       c.euro_purchase_price*d.quantity AS products_cost,
      d.quantity,
CASE WHEN d.quantity > 1 THEN 'multi' ELSE 'mono' END AS mono_multi,
0 AS msg_perso

FROM inter.tags t
JOIN inter.order_details d ON d.id = t.link_id AND d.dw_country_code = t.dw_country_code
JOIN `sales.shop_orders_margin` som ON som.order_id = d.order_id AND som.dw_country_code = t.dw_country_code
JOIN `inter.shipping_modes` sm ON sm.id = som.shipping_mode_id AND sm.dw_country_code = som.dw_country_code
JOIN product.catalog AS c ON d.dw_country_code = c.dw_country_code AND c.product_id = d.product_id
LEFT JOIN pot_shop_reexp_shipments psc ON psc.order_id = d.order_id AND psc.order_detail_id = d.id AND psc.dw_country_code = d.dw_country_code 
LEFT JOIN ops.shipping_costs sc ON COALESCE(psc.shipping_date, DATE(t.timestamp)) >= sc.date_start AND (sc.date_end IS NULL OR COALESCE(psc.shipping_date, DATE(t.timestamp)) <= sc.date_end) AND (d.quantity * COALESCE(c.weight, 0.2) + 0.15)/1000 >= sc.min_weight AND (sc.max_weight IS NULL OR (d.quantity * COALESCE(c.weight, 0.2) + 0.15)/1000 < sc.max_weight) AND sc.shipping_mode_id = som.shipping_mode_id
LEFT JOIN ops.shipping_mode_nicenames smn ON smn.shipping_mode_id = som.shipping_mode_id
LEFT JOIN ops.logistics_costs dlc_first_product ON dlc_first_product.name = 'picking first article' AND COALESCE(psc.shipping_date, DATE(t.timestamp)) >= DATE(dlc_first_product.date_start) AND (COALESCE(psc.shipping_date, DATE(t.timestamp)) <= DATE(dlc_first_product.date_end) OR dlc_first_product.date_end IS NULL)
  LEFT JOIN ops.logistics_costs dlc_next_product ON dlc_next_product.name = 'picking next article' AND COALESCE(psc.shipping_date, DATE(t.timestamp)) >= DATE(dlc_next_product.date_start) AND (COALESCE(psc.shipping_date, DATE(t.timestamp)) <= DATE(dlc_next_product.date_end) OR dlc_next_product.date_end IS NULL)
  LEFT JOIN ops.logistics_costs dlc_prepacked ON dlc_prepacked.name = 'picking prepacked' AND COALESCE(psc.shipping_date, DATE(t.timestamp)) >= DATE(dlc_prepacked.date_start) AND (COALESCE(psc.shipping_date, DATE(t.timestamp)) <= DATE(dlc_prepacked.date_end) OR dlc_prepacked.date_end IS NULL)
  LEFT JOIN snippets.product_codifications pc ON c.product_codification_id = pc.product_codification_id
  
WHERE t.type = 'DET'
AND t.value = 'reexp'