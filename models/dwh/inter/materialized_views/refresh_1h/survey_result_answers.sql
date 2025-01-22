SELECT 'FR' AS dw_country_code, id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at FROM `bdd_prod_fr.wp_jb_survey_result_answers` 
UNION ALL
SELECT 'DE' AS dw_country_code, id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at FROM `bdd_prod_de.wp_jb_survey_result_answers` 
UNION ALL
SELECT 'ES' AS dw_country_code, id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at FROM `bdd_prod_es.wp_jb_survey_result_answers`  
UNION ALL
SELECT 'IT' AS dw_country_code, id,result_id,question_id,answer_id,null as date,null as ranking,created_at,updated_at FROM `bdd_prod_it.wp_jb_survey_result_answers`  
