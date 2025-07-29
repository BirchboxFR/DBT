
SELECT 
  'FR' as dw_country_code,
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id
FROM `teamdata-291012.prod_fr.wp_jb_survey_result_answers`
WHERE `_ab_cdc_deleted_at` IS NULL

{% if is_incremental() %}
  -- Lookback de 2 heures pour capturer les mises à jour tardives
  AND DATETIME(`_airbyte_extracted_at`) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 HOUR)
{% endif %}


