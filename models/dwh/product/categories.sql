SELECT tx1.dw_country_code,
       CONCAT(REPLACE(t1.name, '&amp;', '&'), ' / ', REPLACE(t2.name, '&amp;', '&'), ' / ', REPLACE(t3.name, '&amp;', '&')) AS product_categories,
       REPLACE(t1.name, '&amp;', '&') AS category_lvl_1,
       REPLACE(t2.name, '&amp;', '&') AS category_lvl_2,
       REPLACE(t3.name, '&amp;', '&') AS category_lvl_3,
       tx3.term_id AS term_id,
       3 AS category_lvl,
       tx2.parent AS parent_lvl1,
       tx3.parent AS parent_lvl2
FROM inter.term_taxonomy tx1
INNER JOIN inter.terms t1 ON t1.term_id = tx1.term_id AND t1.dw_country_code = tx1.dw_country_code
LEFT JOIN inter.term_taxonomy tx2 ON tx2.parent = tx1.term_id AND tx2.dw_country_code = tx1.dw_country_code AND tx2.taxonomy = 'product-categories'
LEFT JOIN inter.terms t2 ON t2.term_id = tx2.term_id AND t2.dw_country_code = tx2.dw_country_code
LEFT JOIN inter.term_taxonomy tx3 ON tx3.parent = tx2.term_id AND tx3.dw_country_code = tx2.dw_country_code AND tx3.taxonomy = 'product-categories'
LEFT JOIN inter.terms t3 ON t3.term_id = tx3.term_id AND t3.dw_country_code = tx3.dw_country_code
WHERE tx1.taxonomy = 'product-categories'
  AND tx1.parent = 0
  AND tx3.term_id IS NOT NULL
UNION ALL
SELECT tx1.dw_country_code,
       CONCAT(REPLACE(t1.name, '&amp;', '&'), ' / ', REPLACE(t2.name, '&amp;', '&')) AS product_categories,
       REPLACE(t1.name, '&amp;', '&') AS category_lvl_1,
       REPLACE(t2.name, '&amp;', '&') AS category_lvl_2,
       '' AS category_lvl_3,
       tx2.term_id AS term_id,
       2 AS category_lvl,
       tx2.parent AS parent_lvl1,
       tx2.term_id AS parent_lvl2
FROM inter.term_taxonomy tx1
INNER JOIN inter.terms t1 ON t1.term_id = tx1.term_id AND t1.dw_country_code = tx1.dw_country_code
LEFT JOIN inter.term_taxonomy tx2 ON tx2.parent = tx1.term_id AND tx2.taxonomy = 'product-categories' AND tx2.dw_country_code = tx1.dw_country_code
LEFT JOIN inter.terms t2 ON t2.term_id = tx2.term_id AND t2.dw_country_code = tx2.dw_country_code
WHERE tx1.taxonomy = 'product-categories'
  AND tx1.parent = 0
  AND tx2.term_id IS NOT NULL
UNION ALL
SELECT tx1.dw_country_code,
       CONCAT(REPLACE(t1.name, '&amp;', '&')) AS product_categories,
       REPLACE(t1.name, '&amp;', '&') AS category_lvl_1,
       '' AS category_lvl_2,
       '' AS category_lvl_3,
       tx1.term_id AS term_id,
       1 AS category_lvl,
       tx1.term_id AS parent_lvl1,
       NULL AS parent_lvl2
FROM inter.term_taxonomy tx1
INNER JOIN inter.terms t1 ON t1.term_id = tx1.term_id AND t1.dw_country_code = tx1.dw_country_code
WHERE tx1.taxonomy = 'product-categories'
  AND tx1.parent = 0
  AND tx1.term_id IS NOT NULL
