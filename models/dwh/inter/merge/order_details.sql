{{ config(
    partition_by={
      "field": "_airbyte_extracted_at", 
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["dw_country_code", "id"]
) }}

{%- set countries = var('survey_countries') -%}

--- partie pays

{%- set delete_hooks = [] -%}
{%- for country in countries -%}
  {%- set delete_sql -%}
DELETE FROM `teamdata-291012.{{ country.dataset }}.wp_jb_order_details` 
WHERE (id) IN (
  SELECT CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.id') AS INT64)
  FROM `teamdata-291012.airbyte_internal.{{ country.dataset }}_raw__stream_wp_jb_order_details`
  WHERE JSON_EXTRACT_SCALAR(_airbyte_data, '$._ab_cdc_deleted_at') IS NOT NULL
    AND _airbyte_extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
)
  {%- endset -%}
  {%- do delete_hooks.append(delete_sql) -%}
{%- endfor -%}

{{ config(
    post_hook=delete_hooks
) }}
-- debug

{%- for country in countries %}
SELECT 
  '{{ country.code }}' as dw_country_code,
  b.*
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_order_details` b
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND `_airbyte_extracted_at` >= timestamp_SUB(CURRENT_timestamp(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}

