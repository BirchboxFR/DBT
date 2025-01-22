SELECT 'IT' AS dw_country_code,updated_at, NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type FROM bdd_prod_it.wp_jb_survey_questions 
UNION ALL
SELECT 'DE' AS dw_country_code,updated_at, NULL AS comments, question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type FROM `bdd_prod_de.wp_jb_survey_questions` 
UNION ALL
SELECT 'ES' AS dw_country_code ,updated_at, NULL AS comments, NULL AS question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, NULL AS category_id, title, parent_id, type FROM `bdd_prod_es.wp_jb_survey_questions` 
UNION ALL
SELECT 'FR' AS dw_country_code,updated_at, comments, question_category_id, display_inline, sort_order, id, survey_id, visible, created_at, shuffle, intro, category_id, title, parent_id, type FROM `bdd_prod_fr.wp_jb_survey_questions`