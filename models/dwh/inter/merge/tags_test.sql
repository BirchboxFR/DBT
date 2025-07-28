-- Configuration dans le yml, plus besoin de config() ici
{% set delete_hook %}
  {% if is_incremental() %}
    {{ log("🗑️ DEBUT POST-HOOK: Suppression des IDs détectés", info=true) }}
    
    DELETE FROM `teamdata-291012.prod_fr.tags_test`
    WHERE id IN (
      SELECT DISTINCT CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64)
      FROM `teamdata-291012.airbyte_internal.prod_fr_raw__stream_wp_jb_tags`
      WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') IS NOT NULL
        AND _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        AND JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
    ) AND dw_country_code = 'FR';
    
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

