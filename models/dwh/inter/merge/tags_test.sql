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

-- Simple SELECT des données actuelles (sans post-hook)
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
