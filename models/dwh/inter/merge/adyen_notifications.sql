{# ==========================
   PARAMÈTRES À CHANGER
   ========================== #}
{%- set source_table = "wp_jb_adyen_notifications" -%}   
{%- set target_table = "inter.adyen_notifications" -%}   
{%- set countries = var('survey_countries') -%}
{%- set window_hours = 4 -%}
{%- set window_start -%}
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ window_hours }} HOUR)
{%- endset -%}

{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['dw_country_code','id'],
  partition_by={"field": "_airbyte_extracted_at", "data_type": "timestamp", "granularity": "day"},
  cluster_by=["dw_country_code","id"],
  post_hook=[
    "{% if is_incremental() %} {{ delete_soft_deleted(var('survey_countries'), '" ~ source_table ~ "', '" ~ window_start ~ "') }} {% endif %}"
  ]
) }}

{# ---------- BUILD ---------- #}
{%- if is_incremental() -%}

  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(b.id AS INT64) AS id,
    b.* EXCEPT(id)
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
    AND b._airbyte_extracted_at >= {{ window_start }}
  UNION ALL
  {%- endfor %}


  SELECT
    'FR' AS dw_country_code,
    CAST(a.id AS INT64) AS id,
    a.* EXCEPT(id),'','',''
  FROM `teamdata-291012.inter_archives.wp_jb_adyen_notifications_2024` AS a
  WHERE NULLIF(a._ab_cdc_deleted_at, '') IS NULL
  UNION ALL
  SELECT
    'FR' AS dw_country_code,
    CAST(a.id AS INT64) AS id,
    a.* EXCEPT(id),'','',''
  FROM `teamdata-291012.inter_archives.wp_jb_adyen_notifications_2025` AS a
  WHERE NULLIF(a._ab_cdc_deleted_at, '') IS NULL

{%- else -%}

  {%- for country in countries %}
  SELECT
    '{{ country.code }}' AS dw_country_code,
    CAST(b.id AS INT64) AS id,
    b.* EXCEPT(id)
  FROM `teamdata-291012.{{ country.dataset }}.{{ source_table }}` AS b
  WHERE NULLIF(b._ab_cdc_deleted_at, '') IS NULL
  UNION ALL
  {%- endfor %}

  {# ARCHIVES FR : 2024 + 2025 #}
  SELECT
    'FR' AS dw_country_code,
    CAST(a.id AS INT64) AS id,
    a.* EXCEPT(id),'','',''
  FROM `teamdata-291012.inter_archives.wp_jb_adyen_notifications_2024` AS a
  WHERE NULLIF(a._ab_cdc_deleted_at, '') IS NULL
  UNION ALL
  SELECT
    'FR' AS dw_country_code,
    CAST(a.id AS INT64) AS id,
    a.* EXCEPT(id),'','',''
  FROM `teamdata-291012.inter_archives.wp_jb_adyen_notifications_2025` AS a
  WHERE NULLIF(a._ab_cdc_deleted_at, '') IS NULL

{%- endif %}