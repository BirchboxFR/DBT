CREATE OR REPLACE TABLE inter_tmp.products_stock_log AS 
SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_products_stock_log` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_products_stock_log` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_products_stock_log` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_products_stock_log`