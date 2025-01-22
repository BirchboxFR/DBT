SELECT 'FR' AS dw_country_code, t.*except(date), 
safe_cast(date as date) as date FROM `bdd_prod_fr.wp_jb_trackings` t
UNION ALL
SELECT 'DE' AS dw_country_code,  t.*except(date), 
safe_cast(date as date) as date FROM `bdd_prod_de.wp_jb_trackings` t
UNION ALL
SELECT 'ES' AS dw_country_code,  t.*except(date), 
safe_cast(date as date) as date FROM `bdd_prod_es.wp_jb_trackings` t
UNION ALL
SELECT 'IT' AS dw_country_code,  t.*except(date), 
safe_cast(date as date) as date FROM `bdd_prod_it.wp_jb_trackings` t
