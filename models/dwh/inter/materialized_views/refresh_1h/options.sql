SELECT 'FR' AS dw_country_code, t.* FROM `bdd_prod_fr.wp_options` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.* FROM `bdd_prod_de.wp_options` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.* FROM `bdd_prod_es.wp_options` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.* FROM `bdd_prod_it.wp_options` t
