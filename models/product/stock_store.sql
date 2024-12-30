
WITH stock as (
SELECT p.dw_country_code, sp.store_id, c.sku, c.pc_cat1, c.pc_cat2, c.codification, 
c.product_id, c.brand_full_name, c.product_nice_name AS name, 
sp.stock AS stock, c.sale_price, c.euro_purchase_price, 
FROM inter.store_products sp
JOIN inter.products p ON p.id = sp.product_id AND p.dw_country_code = 'FR'
JOIN {{ ref('catalog') }} c ON c.product_id = p.id AND c.dw_country_code = p.dw_country_code
WHERE c.company_id = 1
AND sp.dw_country_code = 'FR'
)


SELECT stock.*, COALESCE(stock.stock,0)*COALESCE(euro_purchase_price,0) AS valo_stock
FROM stock
