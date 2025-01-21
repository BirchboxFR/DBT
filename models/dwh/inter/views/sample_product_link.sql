SELECT 'FR' AS dw_country_code,t.* FROM `bdd_prod_fr.sample_product_link` t
UNION ALL
SELECT 'DE' AS dw_country_code,t.* FROM `bdd_prod_de.sample_product_link` t
UNION ALL
SELECT 'ES' AS dw_country_code,t.* FROM `bdd_prod_es.sample_product_link` t
UNION ALL
SELECT 'IT' AS dw_country_code,t.* FROM `bdd_prod_it.sample_product_link` t