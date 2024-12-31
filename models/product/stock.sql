
WITH stock as (
SELECT p.dw_country_code, c.sku, c.pc_cat1, c.pc_cat2, c.codification, 
c.product_id, c.brand_full_name, c.product_nice_name AS name, 
p.stock_scamp AS stock, c.sale_price, c.euro_purchase_price, COALESCE(b.box_reservations,0) + COALESCE(b.other_reservations,0) + COALESCE(b.gws_reservations,0) AS bank_reservations, 
  CASE WHEN p.stock_scamp - coalesce(b.box_reservations,0)-coalesce(b.other_reservations, 0) - coalesce(b.gws_reservations,0) < 0 THEN 0 ELSE  p.stock_scamp - coalesce(b.box_reservations,0)-coalesce(b.other_reservations, 0) - coalesce(b.gws_reservations,0) END AS available_stock
FROM {{ ref('products') }} p
JOIN {{ ref('catalog') }} c ON c.product_id = p.id AND c.dw_country_code = p.dw_country_code
LEFT JOIN (SELECT b.sku, SUM(b.box_reservations) AS box_reservations, SUM(b.other_reservations) AS other_reservations, SUM(b.gws_reservations) AS gws_reservations FROM product.bank b GROUP BY b.sku) AS b ON b.sku = c.sku AND c.dw_country_code = 'FR'
WHERE c.company_id = 1
),

position_details AS (
SELECT dw_country_code, sku, min(pwl.dluo) AS dluo_min,
SUM(CASE WHEN zone_key IN ('Z1', 'Z2') THEN stock ELSE 0 END) AS stock_POT1,
SUM(CASE WHEN zone_key = 'Z3' THEN stock ELSE 0 END) AS stock_POT2

FROM {{ ref('product_warehouse_location') } pwl
WHERE pwl.created_at >= CURRENT_DATE
AND stock > 0
GROUP BY dw_country_code, sku
)




SELECT stock.*, COALESCE(stock.stock,0)*COALESCE(euro_purchase_price,0) AS valo_stock, COALESCE(available_stock,0)*COALESCE(euro_purchase_price,0) AS valo_available_stock,
DATE(position_details.dluo_min) AS dluo_min, position_details.stock_POT1, position_details.stock_POT2,
apd.product_class, apd.stock_coverage
FROM stock
LEFT JOIN position_details ON position_details.dw_country_code = stock.dw_country_code AND position_details.sku = stock.sku
LEFT JOIN `appro.prev_appro_details` apd ON apd.sku = stock.sku AND stock.dw_country_code = 'FR'
