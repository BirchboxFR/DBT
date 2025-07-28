-- Configuration dans le yml, plus besoin de config() ici
{{ config(
    partition_by={
      "field": "link_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 100000
      }
    },
    cluster_by=['dw_country_code']
) }}

{% set lookback_hours = 2 %}
{% set countries = [
  {'code': 'FR', 'schema': 'prod_fr', 'raw_table': 'prod_fr_raw__stream_wp_jb_tags'}
] %}

WITH 
-- Nouvelles données (mises à jour récentes)
new_data AS (
  {% for country in countries %}
  SELECT 
    '{{ country.code }}' AS dw_country_code,
    t.id,
    t.link_id,
    t.value,
    t.type,
    t.timestamp,
    t.user_id,
    t._airbyte_extracted_at
  FROM `{{ country.schema }}.wp_jb_tags` t
  WHERE 
    {% if is_incremental() %}
    -- En incrémental : seulement les données récentes
    t._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    {% else %}
    -- Premier run : toute la table
    TRUE
    {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

{% if is_incremental() %}
-- IDs supprimés récemment
deleted_ids AS (
  {% for country in countries %}
  SELECT DISTINCT
    '{{ country.code }}' AS dw_country_code,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64) AS id
  FROM `teamdata-291012.airbyte_internal.{{ country.raw_table }}`
  WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') IS NOT NULL
    AND _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    AND JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

final_data AS (
  -- 1. Nouvelles données
  SELECT * FROM new_data
  
  UNION ALL
  
  -- 2. Données existantes qui ne sont NI supprimées NI mises à jour
  SELECT 
    existing.dw_country_code,
    existing.id,
    existing.link_id,
    existing.value,
    existing.type,
    existing.timestamp,
    existing.user_id,
    existing._airbyte_extracted_at
  FROM {{ this }} existing
  LEFT JOIN deleted_ids del ON existing.id = del.id AND existing.dw_country_code = del.dw_country_code
  LEFT JOIN new_data newd ON existing.id = newd.id AND existing.dw_country_code = newd.dw_country_code
  WHERE del.id IS NULL      -- Pas supprimé récemment
    AND newd.id IS NULL      -- Pas mis à jour récemment
)
{% else %}
final_data AS (
  SELECT * FROM new_data
)
{% endif %}

SELECT 
  dw_country_code,
  id,
  link_id,
  value,
  type,
  timestamp,
  user_id,
  _airbyte_extracted_at
FROM final_data