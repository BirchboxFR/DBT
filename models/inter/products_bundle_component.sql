
SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_products_bundle_component` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_products_bundle_component` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_products_bundle_component` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_products_bundle_component`