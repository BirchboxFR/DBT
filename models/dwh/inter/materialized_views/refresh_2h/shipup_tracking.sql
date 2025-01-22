SELECT 'FR' dw_country_code,* FROM `teamdata-291012.bdd_prod_fr.wp_jb_shipup_tracking` 
union all
SELECT 'DE' dw_country_code,* FROM `teamdata-291012.bdd_prod_de.wp_jb_shipup_tracking` 
union all
SELECT 'ES' dw_country_code,* FROM `teamdata-291012.bdd_prod_es.wp_jb_shipup_tracking`
union all
SELECT 'IT' dw_country_code,* FROM `teamdata-291012.bdd_prod_it.wp_jb_shipup_tracking`