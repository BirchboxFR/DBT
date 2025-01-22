SELECT 'IT' AS dw_country_code, order_detail_id, survey_id, sub_id, order_id, status, timestamp, id, datetime(updated_at)updated_at, user_id FROM `bdd_prod_it.wp_jb_survey_results` 
UNION ALL
SELECT 'DE' AS dw_country_code, order_detail_id, survey_id, sub_id, order_id, status, timestamp, id, datetime(updated_at)updated_at, user_id FROM `bdd_prod_de.wp_jb_survey_results` 
where _sdc_deleted_at is null
UNION ALL
SELECT 'ES' AS dw_country_code, order_detail_id, survey_id, sub_id, order_id, status, timestamp, id, datetime(updated_at)updated_at, user_id FROM `bdd_prod_es.wp_jb_survey_results` 
where _sdc_deleted_at is null
UNION ALL
SELECT 'FR' AS dw_country_code, order_detail_id, survey_id, sub_id, order_id, status, timestamp, id, datetime(updated_at)updated_at, user_id FROM `bdd_prod_fr.wp_jb_survey_results` 
where _sdc_deleted_at is null