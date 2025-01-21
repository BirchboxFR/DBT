SELECT 'FR' AS dw_country_code, t.* FROM `bdd_prod_fr.wp_jb_saved_cart_details` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.* FROM `bdd_prod_de.wp_jb_saved_cart_details` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.* FROM `bdd_prod_es.wp_jb_saved_cart_details` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.* FROM `bdd_prod_it.wp_jb_saved_cart_details` t