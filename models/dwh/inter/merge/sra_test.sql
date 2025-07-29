-- models/wp_jb_survey_result_answers.sql
{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    post_hook="
      delete from teamdata-291012.prod_fr.wp_jb_survey_result_answers
      where id in (
        select cast(json_extract_scalar(_airbyte_data, '$.id') as int64) as id
        from teamdata-291012.airbyte_internal.prod_fr_raw__stream_wp_jb_survey_result_answers
        where _ab_cdc_deleted_at is not null
          and _airbyte_extracted_at >= timestamp_sub(current_timestamp(), interval 2 hour)
      )
    "
  )
}}

select
  id,
  date,
  ranking,
  answer_id,
  result_id,
  created_at,
  updated_at,
  question_id,
  _ab_cdc_updated_at as last_modified
from teamdata-291012.prod_fr.wp_jb_survey_result_answers
where 
  -- Seulement les 2 dernières heures
  _airbyte_extracted_at >= timestamp_sub(current_timestamp(), interval 2 hour)
  -- On garde tous les enregistrements (non supprimés dans la table transformée)
  and _ab_cdc_deleted_at is null

{% if is_incremental() %}
  -- En incrémental, prendre seulement les changements depuis la dernière exécution
  and _ab_cdc_updated_at > (select coalesce(max(last_modified), '1900-01-01') from {{ this }})
{% endif %}