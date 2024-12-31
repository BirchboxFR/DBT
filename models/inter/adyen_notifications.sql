CREATE OR REPLACE TABLE inter_tmp.adyen_notifications 
partition by RANGE_BUCKET(success,GENERATE_ARRAY(0, 1, 1))
cluster by eventcode
AS 
SELECT 'FR' AS dw_country_code, * FROM `bdd_prod_fr.wp_jb_adyen_notifications` 
UNION ALL 
SELECT 'DE' AS dw_country_code, * FROM `bdd_prod_de.wp_jb_adyen_notifications` 
UNION ALL 
SELECT 'ES' AS dw_country_code, * FROM `bdd_prod_es.wp_jb_adyen_notifications` 
UNION ALL 
SELECT 'IT' AS dw_country_code, * FROM `bdd_prod_it.wp_jb_adyen_notifications`


