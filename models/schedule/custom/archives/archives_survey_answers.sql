{{ config(
    materialized='table'
) }}

SELECT 
  'FR' as dw_country_code,
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM `teamdata-291012.Archives.survey_result_answers_archives_box_2022`

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM `teamdata-291012.Archives.survey_result_answers_archives_box_2017`

UNION ALL

SELECT 
  'FR' as dw_country_code,
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id,
  CURRENT_TIMESTAMP() as _airbyte_extracted_at
FROM `teamdata-291012.Archives.wp_jb_survey_result_answers_archives_backup_2025`
