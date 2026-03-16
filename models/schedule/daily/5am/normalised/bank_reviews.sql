SELECT sku, AVG(rating) rating, count(*) nb_reviews
FROM
(
SELECT b.sku,rb.review_id, rb.rating
FROM catalog.bank b 
JOIN {{ ref('reviews_by_user') }} rb ON rb.sku = b.sku

UNION ALL

SELECT b.sku, rb.review_id, rb.rating
FROM catalog.bank b 
JOIN `teamdata-291012.bdd_prod_sublissim.inventory_item` ii ON ii.sku = b.sku
JOIN `teamdata-291012.bdd_prod_sublissim.related_inventory_items` rii ON rii.inventory_item_id = ii.id
JOIN `teamdata-291012.bdd_prod_sublissim.inventory_item` ii_fs ON ii_fs.id = rii.related_inventory_item_id
JOIN {{ ref('reviews_by_user') }}rb ON rb.fz_sku = ii_fs.sku
GROUP BY ALL
)t
GROUP BY ALL