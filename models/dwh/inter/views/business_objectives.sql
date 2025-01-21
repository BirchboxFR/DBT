SELECT 'FR' AS dw_country_code,t.* FROM `bdd_prod_fr.business_objectives` t
UNION ALL
SELECT 'DE' AS dw_country_code,t.* FROM `bdd_prod_de.business_objectives` t
UNION ALL
SELECT 'ES' AS dw_country_code,t.* FROM `bdd_prod_es.business_objectives` t
UNION ALL
SELECT 'IT' AS dw_country_code,t.* FROM `bdd_prod_it.business_objectives` t