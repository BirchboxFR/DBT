
select inventory_item_id from (
SELECT p.dw_country_code, ii.sku, i.stock AS inventory_stock, p.stock_scamp AS product_stock_scamp, date(i.updated_at) as date, ii.id as inventory_item_id, i.stock - p.stock_scamp as delta
FROM bdd_prod_sublissim.inventory_item ii
JOIN bdd_prod_sublissim.inventory i ON i.inventory_item_id = ii.id
JOIN inter.products p ON p.sku = ii.sku

WHERE i.stock <> p.stock_scamp
ORDER BY i.updated_at DESC)