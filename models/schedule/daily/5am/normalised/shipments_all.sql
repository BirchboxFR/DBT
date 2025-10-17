{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true},
    on_schema_change='ignore' 

) }}

SELECT 
"BOX" as shipment_type,
dw_country_code,
box_date,
year as shipping_year, 
month AS shipping_month, 
shipping_date,
first_reexp, 
daily_monthly,
mono_multi, 
shipping_mode_nice_name, 
range_of_weight, 
min_weight_range, 
max_weight_range,
avg(order_weight) AS avg_order_weight,
avg(gws_weight) AS avg_gws_weight,
avg(box_weight) AS avg_box_weight,
count(*) AS nb_orders, 
SUM(shipping_transport_cost) AS shipping_transport_cost,
SUM(total_shipping_taxes) AS total_shipping_taxes,
SUM(shipping_cost) AS shipping_cost,
SUM(picking_cost) AS total_picking_cost,
SUM(product_cost) AS product_cost,
SAFE_DIVIDE(SUM(shipping_transport_cost), count(*)) AS avg_shipping_cost,
SAFE_DIVIDE(SUM(picking_cost), count(*)) AS avg_picking_cost
FROM {{ ref('box_shipments') }}
WHERE year >= 2018
AND shipping_date <= CURRENT_DATE
GROUP BY shipment_type, dw_country_code,shipping_mode_nice_name, range_of_weight,min_weight_range, max_weight_range, first_reexp, daily_monthly,mono_multi, box_date, year, month, shipping_date

UNION ALL

SELECT 
"SHOP" as shipment_type,
dw_country_code,
NULL as box_date,
year as shipping_year, 
month AS shipping_month, 
shipping_date,
first_reexp,
NULL as daily_monthly,
NULL as mono_multi, 
if(shipping_mode_nice_name="Asendia", "Colissimo",shipping_mode_nice_name) as shipping_mode_nice_name, 
range_of_weight, 
min_weight_range, 
max_weight_range,
avg(order_weight) AS avg_order_weight,
NULL AS avg_gws_weight,
NULL AS avg_box_weight,
count(*) AS nb_orders, 
SUM(shipping_transport_cost) AS shipping_transport_cost,
SUM(total_shipping_taxes) AS total_shipping_taxes,
SUM(shipping_cost) AS shipping_cost,
SUM(picking_cost) AS total_picking_cost,
SUM(products_cost) AS product_cost,
SAFE_DIVIDE(SUM(shipping_transport_cost), count(*)) AS avg_shipping_cost,
SAFE_DIVIDE(SUM(picking_cost), count(*)) AS avg_picking_cost
FROM {{ ref('shop_shipments') }}
WHERE year >= 2018
AND shipping_date <= CURRENT_DATE
GROUP BY shipment_type, dw_country_code,shipping_mode_nice_name, range_of_weight,min_weight_range, max_weight_range, first_reexp,  year, month, shipping_date

ORDER BY shipping_year, shipping_month, box_date, shipping_mode_nice_name, range_of_weight
