{{
  config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
       "granularity": "month"
    },
    cluster_by=["dw_country_code"]
  )
}}


WITH monthly_avg_cost AS (
SELECT r.authorized_country, campaign_date, variation_type, AVG(ii.euro_purchase_price) AS monthly_cost
FROM `teamdata-291012.Spreadsheet_synchro.raw_doc_compo2` r
JOIN `teamdata-291012.catalog.inventory_item_catalog` ii ON ii.sku = r.sku_compo
WHERE campaign_date >= '2024-01-01'
AND variation_type = 'MONTHLY'
AND r.authorized_country IS NOT NULL
GROUP BY ALL
ORDER BY campaign_date DESC
) 

SELECT 
    bs.dw_country_code,
    bs.date as month, 
    date(payment_date) as date,
    round(sum(COALESCE(p.attr_discounted_purchase_price, p.attr_purchase_price)),1) as cost,
    coupon,
    'Discount' as type,
    'BOX' as product_type,
    'ACQUIZ' as acquis_type,
    shipping_Date as first_day,
    CASE WHEN bs.crm_acquisition THEN 'CRM' ELSE 'OTHER' END AS attribution
FROM sales.box_sales bs 
LEFT JOIN inter.coupons c 
    ON c.code = bs.coupon 
    AND c.dw_country_code = bs.dw_country_code 
LEFT JOIN inter.products p 
    ON c.discount_type = 'PRODUCT' 
    AND c.discount_amount = CAST(p.id AS STRING)
    AND p.dw_country_code = c.dw_country_code
JOIN inter.boxes b 
    ON b.dw_country_code = bs.dw_country_code 
    AND b.date = bs.date 
WHERE coupon is not null and bs.year>2021
GROUP BY ALL 


UNION ALL


SELECT bs.dw_country_code, 
r.campaign_date,
date(payment_date) as date, 
ROUND(SUM(CASE WHEN ii.euro_purchase_price-monthly_avg_cost.monthly_cost > 0 THEN ii.euro_purchase_price-monthly_avg_cost.monthly_cost ELSE 0 END),0) AS cost, 
CAST(NULL AS STRING) AS coupon,
'Acquis box' AS type,
'BOX' AS product_type,
'ACQUIZ' AS acquis_type,
shipping_Date as first_day,
CASE WHEN bs.crm_acquisition THEN 'CRM' ELSE 'OTHER' END AS attribution
FROM `teamdata-291012.Spreadsheet_synchro.raw_doc_compo2` r
JOIN monthly_avg_cost USING (campaign_date, authorized_country)
JOIN `teamdata-291012.catalog.inventory_item_catalog` ii ON ii.sku = r.sku_compo
JOIN sales.box_sales bs ON bs.dw_country_code = r.authorized_country AND bs.date = r.campaign_date AND bs.coffret_id = r.coffret_id
JOIN inter.boxes b ON b.dw_country_code = bs.dw_country_code AND b.date = bs.date 

WHERE r.variation_type = 'ACQUIS'
GROUP BY ALL
HAVING cost > 0
