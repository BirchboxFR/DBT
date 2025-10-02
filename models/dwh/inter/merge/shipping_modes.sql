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
   _airbyte_raw_id,
  _airbyte_extracted_at,
  _airbyte_meta,
  _airbyte_generation_id,
  id,

  CASE WHEN CAST(box AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(box AS STRING) = 'MA==' THEN FALSE END AS box,
  fee,
  code,
  name,

  -- rank pas booléen → on le garde brut
  rank,

  CASE WHEN CAST(shop AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(shop AS STRING) = 'MA==' THEN FALSE END AS shop,
  CASE WHEN CAST(type AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(type AS STRING) = 'MA==' THEN FALSE END AS type,
  delay,
  CASE WHEN CAST(store AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(store AS STRING) = 'MA==' THEN FALSE END AS store,
  title,
  CASE WHEN CAST(franco AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(franco AS STRING) = 'MA==' THEN FALSE END AS franco,
  CASE WHEN CAST(status AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(status AS STRING) = 'MA==' THEN FALSE END AS status,
  country,
  CASE WHEN CAST(`default` AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(`default` AS STRING) = 'MA==' THEN FALSE END AS `default`,
  CASE WHEN CAST(tracked AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(tracked AS STRING) = 'MA==' THEN FALSE END AS tracked,

  b2c_code,
  b2c_name,
  max_delay,
  min_delay,
  CASE WHEN CAST(test_mode AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(test_mode AS STRING) = 'MA==' THEN FALSE END AS test_mode,

  b2c_method,
  created_at,
  fee_franco,
  CASE WHEN CAST(franco_sub AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(franco_sub AS STRING) = 'MA==' THEN FALSE END AS franco_sub,
  more_infos,
  updated_at,
  business_days,
  _ab_cdc_cursor,
  _ab_cdc_log_pos,
  _ab_cdc_log_file,
  CASE WHEN CAST(b2c_method_daily AS STRING) = 'MQ==' THEN TRUE 
       WHEN CAST(b2c_method_daily AS STRING) = 'MA==' THEN FALSE END AS b2c_method_daily,
  _ab_cdc_deleted_at,
  _ab_cdc_updated_at,
  shipup_carrier_code,
  shipup_service_code,
  relative_path_to_logo,
  linked_shipping_mode_id
FROM `teamdata-291012.{{ country.dataset }}.wp_jb_shipping_modes` b
WHERE `_ab_cdc_deleted_at` IS NULL
{% if is_incremental() %}
  AND `_airbyte_extracted_at` >= timestamp_SUB(CURRENT_timestamp(), INTERVAL 2 HOUR)
{% endif %}
{{ "UNION ALL" if not loop.last }}
{%- endfor %}

