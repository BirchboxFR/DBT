-- Configuration dans le yml, plus besoin de config() ici
{% set delete_hook %}
  {% if is_incremental() %}
    {{ log("🗑️ DEBUT POST-HOOK: Suppression des IDs détectés", info=true) }}
    
    -- Étape 1: Identifier les IDs à supprimer
    CREATE TEMP TABLE ids_to_delete AS (
      SELECT DISTINCT 
        'FR' AS dw_country_code,
        CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64) AS id
      FROM `teamdata-291012.airbyte_internal.prod_fr_raw__stream_wp_jb_tags`
      WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') IS NOT NULL
        AND _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        AND JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
    );
    
    -- Étape 2: Logger les IDs qui vont être supprimés
    CREATE TEMP TABLE actual_deletes AS (
      SELECT target.id, target.dw_country_code 
      FROM {{ this }} target
      INNER JOIN ids_to_delete del ON target.id = del.id AND target.dw_country_code = del.dw_country_code
    );
    
    -- Cette ligne va afficher dans les logs DBT
    SELECT CONCAT('🗑️ SUPPRESSION: ', STRING_AGG(CAST(id AS STRING), ', ')) as deleted_ids
    FROM actual_deletes;
    
    -- Étape 3: Faire le DELETE
    DELETE FROM {{ this }}
    WHERE EXISTS (
      SELECT 1 FROM ids_to_delete del 
      WHERE del.id = {{ this }}.id AND del.dw_country_code = {{ this }}.dw_country_code
    );
    
    {{ log("🗑️ FIN POST-HOOK: Suppressions terminées", info=true) }}
  {% else %}
    {{ log("⏭️ POST-HOOK SKIP: Premier run, pas de suppressions", info=true) }}
  {% endif %}
{% endset %}

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
    cluster_by=['dw_country_code'],
    post_hook=delete_hook
) }}

{% set lookback_hours = 2 %}
{% set countries = [
  {'code': 'FR', 'schema': 'prod_fr', 'raw_table': 'prod_fr_raw__stream_wp_jb_tags'}
] %}

-- Simple SELECT des données actuelles
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
  t._airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_hours }} HOUR)
  {% else %}
  TRUE
  {% endif %}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

