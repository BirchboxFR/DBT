
{{ config(
    post_hook="DELETE FROM `teamdata-291012.prod_fr.wp_jb_survey_result_answers`
WHERE (id) IN (
  SELECT
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64)
  FROM `teamdata-291012.airbyte_internal.prod_fr_raw__stream_wp_jb_survey_result_answers`
  WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
    AND TIMESTAMP(_airbyte_extracted_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
)"
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
  question_id
FROM `teamdata-291012.prod_fr.wp_jb_survey_result_answers`
WHERE `_ab_cdc_deleted_at` IS NULL

{% if is_incremental() %}
  -- Lookback de 2 heures pour capturer les mises à jour tardives
  AND DATETIME(`_airbyte_extracted_at`) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 HOUR)
{% endif %}


