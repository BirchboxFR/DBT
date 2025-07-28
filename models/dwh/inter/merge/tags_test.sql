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

WITH 
-- Données FR actuelles (non supprimées)
current_data AS (
  SELECT 
    'FR' AS dw_country_code,
    t.id,
    t.link_id,
    t.value,
    t.type,
    t.timestamp,
    t.user_id,
    t._airbyte_extracted_at
  FROM `prod_fr.wp_jb_tags` t
  WHERE 
    {% if is_incremental() %}
    t._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    {% else %}
    TRUE
    {% endif %}
),

{% if is_incremental() %}
-- IDs supprimés détectés dans la table raw pendant la période de lookback
deleted_ids AS (
  SELECT DISTINCT
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64) AS id
  FROM `teamdata-291012.airbyte_internal.prod_fr_raw__stream_wp_jb_tags`
  WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') IS NOT NULL
    AND _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
    -- Filtre pour les événements de suppression
    AND JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
),

-- Garder seulement les données existantes qui ne sont PAS dans les suppressions
final_data AS (
  -- Nouvelles données
  SELECT * FROM current_data
  
  UNION ALL
  
  -- Données existantes qui ne sont pas supprimées
  SELECT 
    existing.dw_country_code,
    existing.id,
    existing.link_id,
    existing.value,
    existing.type,
    existing.timestamp,
    existing.user_id,
    existing._rivery_last_update
  FROM {{ this }} existing
  LEFT JOIN deleted_ids del ON existing.id = del.id
  LEFT JOIN current_data curr ON existing.id = curr.id
  WHERE del.id IS NULL  -- Pas dans les suppressions
    AND curr.id IS NULL -- Pas déjà dans les nouvelles données
    AND existing.dw_country_code = 'FR'
)
{% else %}
final_data AS (
  SELECT * FROM current_data
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
  t._airbyte_extracted_at
FROM final_data