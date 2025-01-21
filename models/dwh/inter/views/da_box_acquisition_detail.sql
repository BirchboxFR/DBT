SELECT 'FR' AS dw_country_code, t.* FROM `bdd_prod_fr.da_box_acquisition_detail` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.* FROM `bdd_prod_de.da_box_acquisition_detail` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.* FROM `bdd_prod_es.da_box_acquisition_detail` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.* FROM `bdd_prod_it.da_box_acquisition_detail` t