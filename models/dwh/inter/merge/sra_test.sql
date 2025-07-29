{%- set countries = [
    {'code': 'FR', 'dataset': 'prod_fr'}
] -%}

{{ config(
    materialized='incremental',
    unique_key=['dw_country_code', 'id'],
    post_hook=[
        {%- for country in countries %}
        "DELETE FROM `teamdata-291012.prod_fr.sra_test`
WHERE dw_country_code = '{{ country.code }}' AND (id) IN (
  SELECT CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64)
  FROM `teamdata-291012.airbyte_internal.{{ country.dataset }}_raw__stream_wp_jb_survey_result_answers`
  WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
    AND TIMESTAMP(_airbyte_extracted_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
)"{{ "," if not loop.last }}
        {%- endfor %}
    ]
) }}

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_survey_result_answers`
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND DATETIME(`_airbyte_extracted_at`) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}