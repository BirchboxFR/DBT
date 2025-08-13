{%- set countries = var('survey_countries') -%}

--- partie pays



{{ log('TARGET => ' ~ target, true) }}
{{ log('THIS => ' ~ this, true) }}
{{ log('COUNTRIES => ' ~ countries | tojson, true) }}

-- debug

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
  question_id,
  _airbyte_extracted_at
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_survey_result_answers`
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND DATETIME(`_airbyte_extracted_at`) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}