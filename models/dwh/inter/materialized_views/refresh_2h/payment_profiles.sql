SELECT 'FR' AS dw_country_code, t.*except(card_validity), 
safe_cast(card_validity as date) as card_validity FROM `bdd_prod_fr.wp_jb_payment_profiles` t
UNION ALL
SELECT 'DE' AS dw_country_code, t.*except(card_validity), 
safe_cast(card_validity as date) as card_validity FROM `bdd_prod_de.wp_jb_payment_profiles` t
UNION ALL
SELECT 'ES' AS dw_country_code, t.*except(card_validity), 
safe_cast(card_validity as date) as card_validity FROM `bdd_prod_es.wp_jb_payment_profiles` t
UNION ALL
SELECT 'IT' AS dw_country_code, t.*except(card_validity), 
safe_cast(card_validity as date) as card_validity FROM `bdd_prod_it.wp_jb_payment_profiles` t