SELECT 'FR' AS dw_country_code, t.* FROM `bdd_prod_fr.ga_transactions` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.* FROM `bdd_prod_de.ga_transactions` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.* FROM `bdd_prod_es.ga_transactions` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.* FROM `bdd_prod_it.ga_transactions` t